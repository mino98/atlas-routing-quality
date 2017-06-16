"""Calculates the latency of all 1-hop, 2-hops, 3-hops and 4-hops
paths between any pair of probes, using the measurements previously
obtained with the get-measurements.py tool."""

import MySQLdb as mysql
import logging
import sys
import argparse
import collections
from functools import lru_cache

# Logger object
logger = logging.getLogger("calculate-paths")

# Database
sql = mysql.connect(
        host='localhost',
        database='atlas',
        user='...',
        password='...'
    )

def cache_tree():
    """Struct to cache the partial results in memory"""
    return collections.defaultdict(cache_tree)

def prepare_db():
    """(Re-)create the database"""
    cur=sql.cursor()
    cur.execute('''DROP TABLE IF EXISTS results;''')
    sql.commit()

    cur = sql.cursor() 
    cur.execute('''
        CREATE TABLE results (
            from_id     INTEGER NOT NULL,
            to_id       INTEGER NOT NULL,
            h1          FLOAT,
            h2          FLOAT,
            h2_path     JSON,
            h3          FLOAT,
            h3_path     JSON,
            h4          FLOAT,
            h4_path     JSON,
            UNIQUE(from_id, to_id)
        );''')
    sql.commit()

def parse():
    """Parse arguments."""
    parser = argparse.ArgumentParser(
        description=sys.modules[__name__].__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)

    g = parser.add_mutually_exclusive_group()
    g.add_argument("--debug", "-d", action="store_true",
                   default=False,
                   help="enable debugging")
    g.add_argument("--silent", "-s", action="store_true",
                   default=False,
                   help="don't log to console")

    return parser.parse_args()

@lru_cache(maxsize=None)
def get_segment_latency(a, b, try_reverse=False):
    """Returns the latency between the probes 'a' and 'b'.
    If 'try_reverse' is True, it will return latency b->a if the measurement
    a->b is unavailable.

    Note: this function uses memoization for this function to speed up 
    the computation."""
    cur = sql.cursor()
    cur.execute("""SELECT min 
        FROM measurements
        WHERE
            from_id = %s 
            AND to_id = %s""", (a, b))
    row = cur.fetchone()
    cur.close()
    if row is None or row[0] is None: 
        if(try_reverse):
            return get_segment_latency(b, a, try_reverse=False)
        else:
            return None
    return row[0]

def calculate_results_h1():
    """Calculates the minimum latency for all 1-hop paths 
    between all pairs of probes."""
    cur1 = sql.cursor()
    cur1.execute('SELECT id FROM probes')
    probes = cur1.fetchall()
    cur1.close()
    for i in probes:
        src = i[0]

        for j in probes:
            dst = j[0]
            if src == dst: continue

            logger.debug("1-hop path {}->{}".format(src, dst))
                
            latency = get_segment_latency(src, dst, try_reverse=True)
            if latency is None: continue
            
            logger.debug("{}->{}: {}".format(src, dst, latency))
            cur2 = sql.cursor()
            cur2.execute('INSERT INTO results (from_id, to_id, h1) VALUES (%s, %s, %s)', (src, dst, latency))
            sql.commit()

def calculate_results_h2():
    """Calculates the minimum latency for all 2-hop paths 
    between all pairs of probes."""

    # Creates a structure where we store the latency of all the paths 
    # we already solved for A->B, so that we don't need to solve them 
    # again for B->A: 
    solved_paths = cache_tree()

    cur1 = sql.cursor()
    cur1.execute('SELECT id FROM probes')

    probes = cur1.fetchall()
    cur1.close()
    for i in probes:
        src = i[0]

        for j in probes:
            dst = j[0]
            if src == dst: continue
            min_latency = None

            # Check if we already solved this path in the reverse direction and, 
            # if so, reuse that result:
            if solved_paths[dst][src]['h2']:
                cur2 = sql.cursor()
                cur2.execute('''UPDATE results 
                    SET h2 = %s, 
                    h2_path = '{"extra-hops": [%s]}' 
                    WHERE from_id = %s AND to_id = %s''',
                    (solved_paths[dst][src]['h2'], 
                    solved_paths[dst][src]['hop1'], 
                    src, 
                    dst))
                cur2.close()
                sql.commit()
                continue

            for k in probes:
                hop1 = k[0]
                if hop1 == src or hop1 == dst: continue 
                
                segment_lat = get_segment_latency(src, hop1, try_reverse=True)
                if segment_lat is None: continue
                latency = segment_lat

                segment_lat = get_segment_latency(hop1, dst, try_reverse=True)
                if segment_lat is None: continue
                latency = latency + segment_lat

                latency = round(latency, 2)

                if(min_latency is None or min_latency > latency):
                    logger.debug("2-hop path {}->{}->{}: {}".format(src, hop1, dst, latency))
                    min_latency = latency
                    solved_paths[src][dst]['h2'] = latency
                    solved_paths[src][dst]['hop1'] = hop1
                    cur2 = sql.cursor()
                    cur2.execute('''UPDATE results
                        SET h2 = %s, 
                        h2_path = '{"extra-hops": [%s]}' 
                        WHERE from_id = %s 
                        AND to_id = %s''', 
                        (latency, hop1, src, dst))
                    cur2.close()
                    sql.commit()

def calculate_results_h3():
    """Calculates the minimum latency for all 3-hop paths 
    between all pairs of probes."""

    # Creates a structure where we store the latency of all the paths 
    # we already solved for A->B, so that we don't need to solve them 
    # again for B->A: 
    solved_paths = cache_tree()

    cur1 = sql.cursor()
    cur1.execute('SELECT id FROM probes')

    probes = cur1.fetchall()
    cur1.close()
    for i in probes:
        src = i[0]

        for j in probes:
            dst = j[0]
            if src == dst: continue
            min_latency = None

            # Check if we already solved this path in the reverse direction and, 
            # if so, reuse that result:
            if solved_paths[dst][src]['h3']:
                cur2 = sql.cursor()
                cur2.execute('''UPDATE results
                    SET h3 = %s, 
                    h3_path = '{"extra-hops": [%s, %s]}' 
                    WHERE from_id = %s AND to_id = %s''',
                    (solved_paths[dst][src]['h3'], 
                    solved_paths[dst][src]['hop1'],
                    solved_paths[dst][src]['hop2'],  
                    src, 
                    dst))
                cur2.close()
                sql.commit()
                continue
            
            for k in probes:
                hop1 = k[0]
                if hop1 == src or hop1 == dst: continue 

                for l in probes:
                    hop2 = l[0]
                    if hop2 == src or hop2 == hop1 or hop2 == dst: continue 

                    segment_lat = get_segment_latency(src, hop1, try_reverse=True)
                    if segment_lat is None: continue
                    latency = segment_lat

                    segment_lat = get_segment_latency(hop1, hop2, try_reverse=True)
                    if segment_lat is None: continue
                    latency = latency + segment_lat

                    segment_lat = get_segment_latency(hop2, dst, try_reverse=True)
                    if segment_lat is None: continue
                    latency = latency + segment_lat
                    
                    latency = round(latency, 2)
   
                    if(min_latency is None or min_latency > latency):
                        logger.debug("3-hop path {}->{}->{}->{}: {}".format(src, hop1, hop2, dst, latency))
                        min_latency = latency
                        solved_paths[src][dst]['h3'] = latency
                        solved_paths[src][dst]['hop1'] = hop1
                        solved_paths[src][dst]['hop2'] = hop2
                        cur2 = sql.cursor()
                        cur2.execute('''UPDATE results 
                            SET h3 = %s, 
                            h3_path = '{"extra-hops": [%s, %s]}' 
                            WHERE from_id = %s 
                            AND to_id = %s''', 
                            (latency, hop1, hop2, src, dst))
                        cur2.close()
                        sql.commit()

def calculate_results_h4():
    """Calculates the minimum latency for all 4-hop paths 
    between all pairs of probes."""

    # Creates a structure where we store the latency of all the paths 
    # we already solved for A->B, so that we don't need to solve them 
    # again for B->A: 
    solved_paths = cache_tree()

    cur1 = sql.cursor()
    cur1.execute('SELECT id FROM probes')

    probes = cur1.fetchall()
    cur1.close()
    for i in probes:
        src = i[0]

        for j in probes:
            dst = j[0]
            if src == dst: continue
            min_latency = None

            # Check if we already solved this path in the reverse direction and, 
            # if so, reuse that result:
            if solved_paths[dst][src]['h4']:
                cur2 = sql.cursor()
                cur2.execute('''UPDATE results 
                    SET h4 = %s, 
                    h4_path = '{"extra-hops": [%s, %s, %s]}' 
                    WHERE from_id = %s AND to_id = %s''',
                    (solved_paths[dst][src]['h4'], 
                    solved_paths[dst][src]['hop1'],
                    solved_paths[dst][src]['hop2'],  
                    solved_paths[dst][src]['hop3'],  
                    src, 
                    dst))
                cur2.close()
                sql.commit()
                continue

            for k in probes:
                hop1 = k[0]
                if hop1 == src or hop1 == dst: continue 

                for l in probes:
                    hop2 = l[0]
                    if hop2 == src or hop2 == hop1 or hop2 == dst: continue 

                    for m in probes:
                        hop3 = m[0]
                        if hop3 == src or hop3 == hop1 or hop3 == hop2 or hop3 == dst: continue 

                        segment_lat = get_segment_latency(src, hop1, try_reverse=True)
                        if segment_lat is None: continue
                        latency = segment_lat

                        segment_lat = get_segment_latency(hop1, hop2, try_reverse=True)
                        if segment_lat is None: continue
                        latency = latency + segment_lat

                        segment_lat = get_segment_latency(hop2, hop3, try_reverse=True)
                        if segment_lat is None: continue
                        latency = latency + segment_lat

                        segment_lat = get_segment_latency(hop3, dst, try_reverse=True)
                        if segment_lat is None: continue
                        latency = latency + segment_lat

                        latency = round(latency,2)

                        if(min_latency is None or min_latency > latency):
                            logger.debug("4-hop path {}->{}->{}->{}->{}: {}".format(src, hop1, hop2, hop3, dst, latency))
                            min_latency = latency
                            solved_paths[src][dst]['h4'] = latency
                            solved_paths[src][dst]['hop1'] = hop1
                            solved_paths[src][dst]['hop2'] = hop2
                            solved_paths[src][dst]['hop3'] = hop3
                            cur2 = sql.cursor()
                            cur2.execute('''UPDATE results
                                SET h4 = %s, 
                                h4_path = '{"extra-hops": [%s, %s, %s]}' 
                                WHERE from_id = %s 
                                AND to_id = %s''', 
                                (latency, hop1, hop2, hop3, src, dst))
                            cur2.close()
                            sql.commit()

if __name__ == "__main__":
    options = parse()

    # Logging
    logger.setLevel(options.debug and logging.DEBUG or
                    options.silent and logging.WARN or logging.INFO)
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(ch)

    try:
        logger.info("Preparing database...")
        prepare_db()

        logger.info("Calculating 1-hop paths...")
        calculate_results_h1()

        logger.info("Calculating 2-hop paths...")
        calculate_results_h2()

        logger.info("Calculating 3-hop paths...")
        calculate_results_h3()

        logger.info("Calculating 4-hop paths...")
        calculate_results_h4()

        logger.info("All done!")
        sql.close()
    except Exception as e:
        logger.exception("%s", e)
        sys.exit(1)
