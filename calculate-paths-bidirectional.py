import MySQLdb as mysql
import logging
import sys
import argparse

# Logger object
logger = logging.getLogger("atlas-routing-quality")

# Database
sql = mysql.connect(
        host='localhost',
        database='atlas',
        user='...',
        password='...'
    )

def prepare_db():
    """(Re-)create the sqlite database"""
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
            h3          FLOAT,
            h4          FLOAT,
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

def calculate_results_h1():
    cur1 = sql.cursor()
    cur1.execute('SELECT id FROM probes')
    for i in range(cur1.rowcount):
        src = cur1.fetchone()[0]

        cur2 = sql.cursor()
        cur2.execute("SELECT id FROM probes WHERE id <> %s", (src,))
        for j in range(cur2.rowcount):
            dst = cur2.fetchone()[0]
            logger.info("1-hop path {}->{}".format(src, dst))
                
            cur3 = sql.cursor()
            cur3.execute('SELECT min FROM measurements WHERE from_id = %s AND to_id = %s', (src, dst))
            latency = cur3.fetchone()[0]

            logger.info("{}->{}: {}".format(src, dst, latency))
            cur3 = sql.cursor()
            cur3.execute('INSERT INTO results (from_id, to_id, h1) VALUES (%s, %s, %s)', (src, dst, latency))
            sql.commit()
        cur2.close()
    cur1.close()

def calculate_results_h2():
    cur1 = sql.cursor()
    cur1.execute('SELECT id FROM probes')
    for i in range(cur1.rowcount):
        src = cur1.fetchone()[0]

        cur2 = sql.cursor()
        cur2.execute("SELECT id FROM probes WHERE id <> %s", (src,))
        for j in range(cur2.rowcount):
            dst=cur2.fetchone()[0]
            
            logger.info("2-hop path {}->{}".format(src, dst))

            cur3 = sql.cursor()
            cur3.execute("SELECT to_id FROM measurements WHERE from_id = %s AND to_id <> %s", (src, dst))
            for k in range(cur3.rowcount):
                intermediate1 = cur3.fetchone()[0]
                
                cur4 = sql.cursor()
                cur4.execute('SELECT min FROM measurements WHERE from_id = %s AND to_id = %s', (src, intermediate1))
                row = cur4.fetchone()
                if row is None or row[0] is None: continue
                latency = row[0]
                cur4.close()

                cur4 = sql.cursor()
                cur4.execute('SELECT min FROM measurements WHERE from_id = %s AND to_id = %s', (intermediate1, dst))
                row = cur4.fetchone()
                if row is None or row[0] is None: continue
                latency = latency + row[0]
                cur4.close()

                cur4 = sql.cursor()
                cur4.execute('SELECT h2 FROM results WHERE from_id = %s AND to_id = %s', (src, dst))
                row = cur4.fetchone()
                if row is None:
                    h2 = 0
                else: 
                    h2 = row[0]
                cur4.close()   

                logger.info("{}->{}->{}: {}".format(src, intermediate1, dst, round(latency,2)))
                if(h2 is None or h2 > latency):
                    cur4 = sql.cursor()
                    cur4.execute('UPDATE results SET h2 = %s WHERE from_id = %s AND to_id = %s', (round(latency,2), src, dst))
                    cur4.close()
                    sql.commit()
            cur3.close()
        cur2.close()
    cur1.close

def calculate_results_h3():
    cur1 = sql.cursor()
    cur1.execute('SELECT id FROM probes')
    for i in range(cur1.rowcount):
        src = cur1.fetchone()[0]

        cur2 = sql.cursor()
        cur2.execute("SELECT id FROM probes WHERE id <> %s", (src,))
        for j in range(cur2.rowcount):
            dst=cur2.fetchone()[0]
            
            logger.info("3-hop path {}->{}".format(src, dst))

            cur3 = sql.cursor()
            cur3.execute("SELECT to_id FROM measurements WHERE from_id = %s AND to_id <> %s", (src, dst))
            for k in range(cur3.rowcount):
                intermediate1 = cur3.fetchone()[0]

                cur4 = sql.cursor()
                cur4.execute("SELECT to_id FROM measurements WHERE from_id = %s AND to_id <> %s AND to_id <> %s", (src, intermediate1, dst))
                for k in range(cur4.rowcount):
                    intermediate2 = cur4.fetchone()[0]

                    cur5 = sql.cursor()
                    cur5.execute('SELECT min FROM measurements WHERE from_id = %s AND to_id = %s', (src, intermediate1))
                    row = cur5.fetchone()
                    if row is None or row[0] is None: continue
                    latency = row[0]
                    cur5.close()

                    cur5 = sql.cursor()
                    cur5.execute('SELECT min FROM measurements WHERE from_id = %s AND to_id = %s', (intermediate1, intermediate2))
                    row = cur5.fetchone()
                    if row is None or row[0] is None: continue
                    latency = latency + row[0]
                    cur5.close()

                    cur5 = sql.cursor()
                    cur5.execute('SELECT min FROM measurements WHERE from_id = %s AND to_id = %s', (intermediate2, dst))
                    row = cur5.fetchone()
                    if row is None or row[0] is None: continue
                    latency = latency + row[0]
                    cur5.close()

                    cur5 = sql.cursor()
                    cur5.execute('SELECT h3 FROM results WHERE from_id = %s AND to_id = %s', (src, dst))
                    row = cur5.fetchone()
                    if row is None:
                        h3 = 0
                    else: 
                        h3 = row[0]
                    cur5.close()       

                    logger.info("{}->{}->{}->{}: {}".format(src, intermediate1, intermediate2, dst, round(latency,2)))
                    if(h3 is None or h3 > latency):
                        cur5 = sql.cursor()
                        cur5.execute('UPDATE results SET h3 = %s WHERE from_id = %s AND to_id = %s', (round(latency,2), src, dst))
                        cur5.close()
                        sql.commit()
                cur4.close()
            cur3.close()
        cur2.close()
    cur1.close

def calculate_results_h4():
    cur1 = sql.cursor()
    cur1.execute('SELECT id FROM probes')
    for i in range(cur1.rowcount):
        src = cur1.fetchone()[0]

        cur2 = sql.cursor()
        cur2.execute("SELECT id FROM probes WHERE id <> %s", (src,))
        for j in range(cur2.rowcount):
            dst=cur2.fetchone()[0]
            
            logger.info("4-hop path {}->{}".format(src, dst))

            cur3 = sql.cursor()
            cur3.execute("SELECT to_id FROM measurements WHERE from_id = %s AND to_id <> %s", (src, dst))
            for k in range(cur3.rowcount):
                intermediate1 = cur3.fetchone()[0]

                cur4 = sql.cursor()
                cur4.execute("SELECT to_id FROM measurements WHERE from_id = %s AND to_id <> %s AND to_id <> %s", (src, intermediate1, dst))
                for k in range(cur4.rowcount):
                    intermediate2 = cur4.fetchone()[0]

                    cur5 = sql.cursor()
                    cur5.execute("SELECT to_id FROM measurements WHERE from_id = %s AND to_id <> %s AND to_id <> %s AND to_id <> %s", (src, intermediate1, intermediate2, dst))
                    for k in range(cur5.rowcount):
                        intermediate3 = cur5.fetchone()[0]

                        cur6 = sql.cursor()
                        cur6.execute('SELECT min FROM measurements WHERE from_id = %s AND to_id = %s', (src, intermediate1))
                        row = cur6.fetchone()
                        if row is None or row[0] is None: continue
                        latency = row[0]
                        cur6.close()

                        cur6 = sql.cursor()
                        cur6.execute('SELECT min FROM measurements WHERE from_id = %s AND to_id = %s', (intermediate1, intermediate2))
                        row = cur6.fetchone()
                        if row is None or row[0] is None: continue
                        latency = latency + row[0]
                        cur6.close()

                        cur6 = sql.cursor()
                        cur6.execute('SELECT min FROM measurements WHERE from_id = %s AND to_id = %s', (intermediate2, intermediate3))
                        row = cur6.fetchone()
                        if row is None or row[0] is None: continue
                        latency = latency + row[0]
                        cur6.close()

                        cur6 = sql.cursor()
                        cur6.execute('SELECT min FROM measurements WHERE from_id = %s AND to_id = %s', (intermediate2, dst))
                        row = cur6.fetchone()
                        if row is None or row[0] is None: continue
                        latency = latency + row[0]
                        cur6.close()

                        cur6 = sql.cursor()
                        cur6.execute('SELECT h4 FROM results WHERE from_id = %s AND to_id = %s', (src, dst))
                        row = cur6.fetchone()
                        if row is None:
                            h4 = 0
                        else: 
                            h4 = row[0]
                        cur6.close()     

                        logger.info("{}->{}->{}->{}->{}: {}".format(src, intermediate1, intermediate2, intermediate3, dst, round(latency,2)))
                        if(h4 is None or h4 > latency):
                            cur6 = sql.cursor()
                            cur6.execute('UPDATE results SET h4 = %s WHERE from_id = %s AND to_id = %s', (round(latency,2), src, dst))
                            cur6.close()
                            sql.commit()
                cur4.close()
            cur3.close()
        cur2.close()
    cur1.close

if __name__ == "__main__":
    options = parse()

    # Logging
    logger.setLevel(options.debug and logging.DEBUG or
                    options.silent and logging.WARN or logging.INFO)
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(levelname)s[%(name)s] %(message)s"))
    logger.addHandler(ch)

    try:
        logger.debug("Start prepare_db()")
        prepare_db()

        logger.debug("Start calculate_results_h1()")
        calculate_results_h1()

        logger.debug("Start calculate_results_h2()")
        calculate_results_h2()

        logger.debug("Start calculate_results_h3()")
        calculate_results_h3()

        logger.debug("Start calculate_results_h4()")
        calculate_results_h4()

        logger.info("All completed!")
        sql.close()
    except Exception as e:
        logger.exception("%s", e)
        sys.exit(1)