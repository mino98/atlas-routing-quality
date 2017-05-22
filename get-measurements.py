"""Randomly select a set of Atlas probes and requests a set of 
latency measurement from each probe to each other probe.

Credentials for RIPE Atlas API can be provided on the command-line or
through ``ATLAS_CREATE_KEY`` and ``ATLAS_DOWNLOAD_KEY`` environment
variables. The latest is only needed if results are not public (the
default).

Parts of this code are vaguely based on the very useful script:
 https://github.com/vincentbernat/ripe-atlas-lowest-latency
by Vincent Bernat."""

import os
import sys
import time
import json
import logging
import argparse
import statistics
import threading
import MySQLdb as mysql
import ripe.atlas.cousteau as atlas
from datetime import datetime, timezone
from ipaddress import ip_network, ip_address

# Logger object
logger = logging.getLogger("atlas-routing-quality")

# Database
sql = mysql.connect(
        host='localhost',
        database='atlas',
        user='...',
        password='...'
    )

def prepare_db(options):
    """(Re-)create the database"""
    cur=sql.cursor()
    cur.execute("""DROP TABLE IF EXISTS probes;""")
    sql.commit()

    cur = sql.cursor() 
    cur.execute("""
        CREATE TABLE probes (
            id          INTEGER PRIMARY KEY, 
            country     TEXT NOT NULL,
            af          ENUM('4', '6') NOT NULL,
            asn         INTEGER NOT NULL,
            address     TEXT NOT NULL,
            raw_json    TEXT NOT NULL
        );""")
    sql.commit()

    cur = sql.cursor() 
    cur.execute("""DROP TABLE IF EXISTS measurements;""")
    sql.commit()
    
    cur = sql.cursor() 
    cur.execute("""
        CREATE TABLE measurements (
            msm         INTEGER,
            msm_timestamp   DATETIME,
            from_id     INTEGER NOT NULL,
            to_id       INTEGER NOT NULL,
            state       ENUM('TO_BE_REQUESTED', 'REQUESTED', 'FETCHED', 'FAILED') NOT NULL,
            sent        INTEGER,
            rcvd        INTEGER,
            min         FLOAT,
            avg         FLOAT,
            mean        FLOAT,
            median      FLOAT,
            stdev       FLOAT,
            max         FLOAT,
            measurement_raw_json    JSON,
            results_raw_json        JSON,
            UNIQUE(from_id, to_id)
        ); """)
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

    g = parser.add_argument_group("Probe selection")
    g.add_argument("--country", metavar="COUNTRY",
                   help="Country code of probes")
    g.add_argument("--family", metavar="FAMILY",
                   choices=(4, 6), default=4,
                   help="IP family of probes (4 or 6)", type=int)

    g = parser.add_argument_group("Atlas API")
    g.add_argument("--api-create-key", metavar="KEY",
                   default=os.environ.get("ATLAS_CREATE_KEY", ""),
                   help="API key for create")
    g.add_argument("--api-download-key", metavar="KEY",
                   default=os.environ.get("ATLAS_DOWNLOAD_KEY", ""),
                   help="API key for download")
    g.add_argument("--public", action="store_true",
                   help="Make the results public")
    g.add_argument("--ping-count", metavar="COUNT",
                   type=int, default=4,
                   help="number of ping requests for each probe")

    parser.add_argument("number", metavar="NUM", type=int,
                        help="Number of probes to request")

    return parser.parse_args()

def get_probes(options):
    """Return a list of probes matching the provided options. 
    Note that we only pick probes that have been stable for 30 days."""    
    filters = {"tags": "system-ipv{}-works,system-ipv{}-stable-30d".format(options.family, options.family),
               "status": 1,
               "is_public": "true"}

    if options.country:
        filters["country_code"] = options.country.upper()

    logger.info("Fetch list of probes (v{}, C: {})".format(
        options.family, 
        options.country or "any"))
    probes = atlas.ProbeRequest(**filters)

    num_found = 0
    for probe in probes:
        logger.debug("Trying probe {} (ASN: {}, Country: {}, Address v{}: {})".format(
            probe["id"],
            probe["asn_v{}".format(options.family)],
            probe["country_code"],
            options.family,
            probe["address_v{}".format(options.family)]))

        if probe["address_v{}".format(options.family)] == None:
            logger.debug("No IPv{} address configured, skipping".format(options.family))
            continue

        response = os.system("ping -c 1 -w 1 -W 250 " + probe["address_v{}".format(options.family)] + " > /dev/null")
        if response == 0:
            cur=sql.cursor()
            cur.execute("""INSERT INTO probes
                (id, country, af, asn, address, raw_json) 
                VALUES (%s, %s, %s, %s, %s, %s);
                """, [
                    probe["id"], 
                    probe["country_code"],
                    str(options.family),
                    probe["asn_v{}".format(options.family)],
                    probe["address_v{}".format(options.family)],
                    json.dumps(probe)
                ])
            sql.commit()

            num_found = num_found + 1

            logger.debug("Ping up, so far found {} probes".format(num_found))
            if num_found >= options.number:
                return
        else:
            logger.debug("No answer to ping")

    logger.error("{} probes were requested, but only {} were found".format(options.number, num_found))
    sys.exit(1)

def define_measurements(options):
    logger.debug("Writing the measurements to database")
    cur1=sql.cursor()
    cur1.execute('SELECT id FROM probes')
    for i in range(cur1.rowcount):
        a = cur1.fetchone()

        cur2=sql.cursor()
        cur2.execute('SELECT id FROM probes')
        for j in range(cur2.rowcount):
            b = cur2.fetchone()

            if a[0] == b[0]: continue

            # If a measurement a-->b is already in database, there's no 
            # need to schedule a measurement b-->a.
            cur3=sql.cursor()
            cur3.execute('''SELECT * FROM measurements
                        WHERE from_id=%s AND to_id=%s''', (
                            b[0],
                            a[0]
                        ))
            if(cur3.rowcount > 0):
                cur3.close()
                continue;
            cur3.close()

            cur3=sql.cursor()
            cur3.execute('''INSERT INTO measurements
                        (from_id, to_id, state)
                        VALUES (%s, %s, %s)''', (
                            a[0],
                            b[0],
                            'TO_BE_REQUESTED',
                        ))
            sql.commit()
        cur2.close()
    cur1.close()
    logger.debug("Measurements written to database")

class send_measures_thread(threading.Thread):
    def __init__(self, options):
        threading.Thread.__init__(self)
        self.options = options
        self.start()

    def run(self):  
        """Get measures for the provided probes for the provided endpoints.
        We use a simple ping.
        """
        self.sql = mysql.connect(
            host='localhost',
            database='atlas',
            user='atlas',
            password='ULmm0F6ZS7NZa4d1'
        )
        logger.debug("Thread send_measures_thread started")

        self.cur1 = self.sql.cursor()
        self.cur1.execute('''SELECT p1.id, p1.address, p2.id, p2.address
            FROM measurements AS m
            INNER JOIN probes AS p1 ON m.from_id=p1.id
            INNER JOIN probes AS p2 ON m.to_id=p2.id
            WHERE m.state="TO_BE_REQUESTED"''')
        for i in range(self.cur1.rowcount):
            row = self.cur1.fetchone()
            # Wait if there are already 95 ongoing measurements, not to incur
            # in Atlas user limits (by default: max 100 ongoing measurements)

            self.cur2 = self.sql.cursor()
            self.cur2.execute('SELECT COUNT(*) FROM measurements WHERE state="REQUESTED"')
            num = self.cur2.fetchone()[0]
            self.cur2.close()
            while num > 95:
                logger.debug("Ongoing measurements: {}, WAITING!".format(num))
                time.sleep(5)
                self.cur2 = self.sql.cursor()
                self.cur2.execute('SELECT COUNT(*) FROM measurements WHERE state="REQUESTED"')
                num = self.cur2.fetchone()[0]
                self.cur2.close()

            logger.debug("Creating measurement from {} to {}".format(row[0], row[2]))

            measurement = atlas.Ping(
                af=options.family,
                target=row[3],
                packets=options.ping_count,
                is_public=options.public,
                description="complete-ping-graph ping from {} to {}".format(row[0], row[2]))
            source = atlas.AtlasSource(type="probes",
                                       requested=1,
                                       value=row[0])
            # start_time = datetime(2017, 5, 12, 16, 0, 0, 0, tzinfo=timezone.utc)
            # request = atlas.AtlasCreateRequest(start_time=start_time,
            request = atlas.AtlasCreateRequest(start_time=datetime.utcnow(),
                                               key=options.api_create_key,
                                               sources=[source],
                                               measurements=[measurement],
                                               is_oneoff=True)

            (success, response) = request.create()
            if success:
                self.cur3 = self.sql.cursor()
                self.cur3.execute('''UPDATE measurements 
                    SET
                        state="REQUESTED", 
                        msm=%s,
                        measurement_raw_json=%s
                    WHERE 
                        from_id=%s AND to_id=%s''', (
                        response['measurements'][0],
                        json.dumps(response),
                        row[0],
                        row[2]
                    ))
                self.sql.commit()

                logger.debug("Measure requests successfully sent: {}".format(response['measurements'][0]))
            else:
                raise RuntimeError("Unable to send measure requests: {}".format(response))
        self.cur1.close()

class fetch_results_thread(threading.Thread):
    def __init__(self, options):
        threading.Thread.__init__(self)
        self.options = options
        self.start()

    def run(self):  
        """Fetch the given measures and save them in the SQL table."""
        self.sql = mysql.connect(
            host='localhost',
            database='atlas',
            user='atlas',
            password='ULmm0F6ZS7NZa4d1'
        )
        logger.debug("Thread fetch_results_thread started")

        while True:
            time.sleep(5)
            self.cur1 = self.sql.cursor()
            self.cur1.execute('SELECT COUNT(*) FROM measurements WHERE state="REQUESTED"')
            num = self.cur1.fetchone()[0]
            self.cur1.close()
            logger.debug("Measurements to be fetched: {}".format(num))

            if num == 0:
                return;            

            self.cur1 = self.sql.cursor()
            self.cur1.execute("SELECT msm FROM measurements WHERE state='REQUESTED'")
            for i in range(self.cur1.rowcount):
                msm = self.cur1.fetchone()[0]

                logger.debug("Fetch metadata for {}".format(msm))
                count = 0
                while True:
                    measurement = atlas.Measurement(id=msm)
                    logger.debug("Current state for {}: {}".format(msm, measurement.status))
                    if (measurement.status == "Stopped" or
                        measurement.status == "Failed" or
                        measurement.status == "No suitable probes"):
                        break
                    count += 1
                    if count % 10 == 0:
                        logger.debug(("Waiting for measurement {} "
                                     "to complete "
                                     "(current state: {})").format(msm, measurement.status))
                    time.sleep(5)

                if (measurement.status == "Failed" or
                    measurement.status == "No suitable probes"):
                        logger.error("Measurement from {} failed".format(msm))
                        self.cur2 = self.sql.cursor()
                        self.cur2.execute('''
                            UPDATE measurements
                            SET 
                                state = 'FAILED'
                            WHERE msm = %s''', [msm])
                        self.sql.commit()
                        self.cur2.close()
                        continue

                logger.debug("Fetch actual results for {}".format(msm))
                success, response = atlas.AtlasLatestRequest(msm_id=msm).create()
                if success:
                    logger.debug("Measure successfully fetched for {}".format(msm))
                    response = response[0]

                    if response["sent"] == 0 or response["rcvd"] == 0:
                        logger.error("Measurement from {} to {} failed: no ping sent or received".format(
                            response["src_addr"], response["dst_addr"]))
                        self.cur2 = self.sql.cursor()
                        self.cur2.execute('''
                            UPDATE measurements
                            SET 
                                state = 'FAILED',
                                msm_timestamp = FROM_UNIXTIME(%s),
                                results_raw_json = %s
                            WHERE msm = %s''', [
                                response["timestamp"],   
                                json.dumps(response),
                                msm
                            ])
                        self.sql.commit()
                        self.cur2.close()
                        continue

                    stats = {}
                    stats['loss'] = (response["sent"] - response["rcvd"]) * 100. / response["sent"]

                    rtts = [y['rtt']
                            for y in response['result']
                            if 'rtt' in y]

                    if rtts:
                        stats['mean'] = statistics.mean(rtts)
                        stats['median'] = statistics.median(rtts)
                        stats['stdev'] = len(rtts) > 1 and statistics.stdev(rtts) or 0
                        stats['min'] = min(rtts)
                        stats['max'] = max(rtts)

                    self.cur2 = self.sql.cursor()
                    self.cur2.execute('''
                        UPDATE measurements
                        SET 
                            state = 'FETCHED',
                            msm_timestamp = FROM_UNIXTIME(%s),
                            sent = %s,
                            rcvd = %s,
                            min = %s,
                            avg = %s,
                            mean = %s,
                            median = %s,
                            stdev = %s,
                            max = %s,
                            results_raw_json = %s
                        WHERE msm = %s''', [
                            response["timestamp"],
                            response["sent"],
                            response["rcvd"],
                            round(stats['min'], 2),
                            round(response["avg"], 2),
                            round(stats['mean'], 2),
                            round(stats['median'], 2),
                            round(stats['stdev'], 2),
                            round(stats['max'], 2),    
                            json.dumps(response),
                            msm
                        ])
                    self.sql.commit()

                    logger.debug("Results {} successfully fetched".format(msm))

                else:
                    raise RuntimeError(
                        "Unable to fetch results for measure {}: {}".format(msm, response))
            self.cur1.close()

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
        prepare_db(options)

        logger.info("Selecting probes...")
        get_probes(options) 

        logger.info("Defining the set of measurements to be made...")
        define_measurements(options)

        logger.info("Starting the measurement thread...")
        t1 = send_measures_thread(options)

        logger.info("Starting the result fetcher...")
        t2 = fetch_results_thread(options)
        
        t1.join()
        t2.join()
        logger.info("All done!")
        sql.close()
    except Exception as e:
        logger.exception("%s", e)
        sys.exit(1)
