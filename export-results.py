import MySQLdb as mysql
import logging
import sys
import argparse
import collections
from functools import lru_cache
from datetime import datetime, timezone, timedelta

# Logger object
logger = logging.getLogger("calculate-paths")

# Database
sql = mysql.connect(
        host='localhost',
        database='atlas',
        user='...',
        password='...'
    )

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

def export_probe_list():
    """Export a file with the details of the probes"""
    fp = open("probes.csv",'w')

    cur1 = sql.cursor()
    cur1.execute("""SELECT id, address, af, asn
                    FROM probes 
                    WHERE id NOT IN (
                      SELECT DISTINCT from_id 
                      FROM `measurements` WHERE state='failed'
                    )
                    ORDER BY id ASC""")
    for i in range(cur1.rowcount):
        row = cur1.fetchone()
        fp.write(','.join([str(item) for item in row]) + "\n") 
    cur1.close()
    fp.close()

def export_matrix():
    """Export a file with a table of all origin-destination pairs"""
    fp = open("matrix.csv",'w')

    cur1 = sql.cursor()
    cur1.execute("""SELECT id 
                    FROM probes 
                    WHERE id NOT IN (
                      SELECT DISTINCT from_id 
                      FROM `measurements` WHERE state='failed'
                    )
                    ORDER BY id ASC""")
    probes = cur1.fetchall()
    cur1.close()    
    for i in probes:
        src = i[0]

        for j in probes:
            dst = j[0]
            if src == dst:
                latency = -1
            else:
                latency = get_segment_latency(src, dst, try_reverse=True)
                if latency is None:
                    raise ValueError("Invalid latency between %d and %d" % (src, dst))
            
            fp.write(str(latency) + ",")
        fp.write("\n") 
    fp.close()

def export_notes():
    """Exports a file with a bunch of notes about this measurement"""
    fp = open("notes.txt",'w')
    fp.write("Measurements notes:\n")

    # Matrix size:
    cur = sql.cursor()
    cur.execute("""SELECT id 
                   FROM probes 
                   WHERE id NOT IN (
                      SELECT DISTINCT from_id 
                      FROM `measurements` WHERE state='failed'
                   )
                   ORDER BY id ASC""")
    fp.write("- matrix size: %dx%d\n" % (cur.rowcount, cur.rowcount))
    cur.close()   

    # Successful measurements:
    cur = sql.cursor()
    cur.execute("""SELECT COUNT(*) FROM `measurements` WHERE state='fetched'""")
    row = cur.fetchone()
    fp.write("- successful measurements: %d\n" % (row[0]))
    cur.close()   

    # Failed measurements:
    cur = sql.cursor()
    cur.execute("""SELECT COUNT(*) FROM `measurements` WHERE state='failed'""")
    row = cur.fetchone()
    fp.write("- failed measurements: %d\n" % (row[0]))
    cur.close()   

    # Start/end time:
    cur = sql.cursor()
    cur.execute("""SELECT 
                   MIN(`results_raw_json`->"$.timestamp") AS t1,
                   MAX(`results_raw_json`->"$.timestamp") AS t2
                   FROM `measurements`;""")
    row = cur.fetchone()
    cur.close()
    if(row[0] != None):
        fp.write("- start time (UTC): %s\n" % datetime.fromtimestamp(int(row[0]), tz=timezone.utc).isoformat())
    if(row[1] != None):
        fp.write("- end time (UTC): %s\n" % datetime.fromtimestamp(int(row[1]), tz=timezone.utc).isoformat()) 

    # List of countries:
    cur = sql.cursor()
    cur.execute("""SELECT DISTINCT (country)
                   FROM probes 
                   WHERE id NOT IN (
                      SELECT DISTINCT from_id 
                      FROM `measurements` WHERE state='failed'
                   )
                   ORDER BY country ASC""")
    row = cur.fetchall()
    cur.close()
    countries = ', '.join([item for sublist in row for item in sublist])
    fp.write("- countries: %s\n" % countries)
    
    # Number of ASNs:
    cur = sql.cursor()
    cur.execute("""SELECT DISTINCT asn
                   FROM probes 
                   WHERE id NOT IN (
                      SELECT DISTINCT from_id 
                      FROM `measurements` WHERE state='failed'
                   )""")
    fp.write("- distinct ASNs: %d\n" % (cur.rowcount))
    cur.close()

    fp.write("- comments: \n")
    fp.close()

if __name__ == "__main__":
    options = parse()

    # Logging
    logger.setLevel(options.debug and logging.DEBUG or
                    options.silent and logging.WARN or logging.INFO)
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(ch)

    try:
        logger.info("Exporting probes list")
        export_probe_list()

        logger.info("Exporting measurements matrix")
        export_matrix()

        logger.info("Generating notes")
        export_notes()

        logger.info("All done")
        sql.close()
    except Exception as e:
        logger.exception("%s", e)
        sys.exit(1)
