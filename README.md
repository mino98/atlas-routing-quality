# Estimate BGP routing "quality" with RIPE Atlas probes
Scripts to verify Internet BGP routing "quality" among a group of random RIPE Atlas probes. 
Use with caution, it's a quick hack so your mileage may vary. 

## 1. Purpose
We use Atlas to measure RTT latency between all pairs of Atlas probes within a small random set. We then calculate for what part of those pairs, an "indirect" path would have lower latency than the direct path.

In other words, we verify whether there are paths between two probes *A* and *B* for which forcing the traffic to route over a third probe *K* (i.e., *A→K→B*) offers better performances than the direct path *A→B*. To do so, we randomly select a small set of probes (e.g., 100) and verify they answer to ICMP echo-requests. Then, we instruct ATLAS to schedule a series of ping from each probe to each other probe. By default we send 4 pings with standard size and consider only the lowest sample. 

Let's define *L(A, B)* as the RTT latency between probes *A* and *B*. For each pair of probes *{A, B}* among the 100 selected:
1.	We set *H1=L(A,B)* is the results of the ATLAS ping. This is the direct path RTT latency.
2.	For each other probe *K* among the remaining 98, we find *H2=min⁡(L(A,K)+L(K,B))* which is the RTT latency of a path with extra hop *K*.
3.	for each other probe J among the remaining 97, we find *H3=min⁡(L(A,K)+L(K,J)+L(J,B))* which is the RTT latency of a path with extra hops K, J.
4.	for each other probe M among the remaining 96, we find *H4=min⁡(L(A,K)+L(K,J)+L(J,M)+L(M,B))* which is the RTT latency of a path with extra hops K, J, M.

Clearly, this experiment results in a large number of pathfinding calculations: finding all routes of up to 3 extra hops between all probe pairs on an overlay with 100 probes results in O(10B) paths calculations. We wrote this simple Python implementation which uses dynamic programming (memoization) to brute-force all calculations. 

That sounds terrible to you? It's good enough for me, a single measurement instance completes in a couple of hours (on one core).

In most measurement instances, we usually found that the direct IP route provides the lowest-latency path in less than half of the paths. Or even less for instances including probes from more than one country, because of the weirdness of international IP transit&peering.

## 2. How it works
The script uses a MySQL database to store the measurement results. Follow these simple steps.

### 2.1 Getting measurements
Once you generated an Atlas API key from the RIPE portal, start a measurement with the `get-measurements.py` script.

To get the full set of options available, do:
```bash
python3 get-measurements.py --help
```

#### 2.1.1 Example 1:
```bash
python3 get-measurements.py --debug --api-create-key XXX-XXX-XXXXXX --family 4 --public 100
```
In this example:
* Use `--family [4|6]` to do measurements over IPv4 or IPv6
* Use `--public` to make all measurements public
* The last parameter (`100`) is the number of probes to pick in the random set
* You can add something like `--country IT` to pick only probes belonging to a single country
* For extra debugging, add `--debug`
* For silent output, add `--silent`

#### 2.1.2 Example 2:
A more convoluted example could be as follows: 
* select 100 Atlas Anchors in a bunch of European Countries
* for an hour (starting 60 minutes from now, so the script has enough time to request all measurements), every 5 minutes we send a train of 6 ping
* pings in each train are separated by 5 seconds
* log everything in `get-measurements.log`, because you never know
```bash
python3 get-measurements.py \
  --debug \
  --api-create-key XXX-XXX-XXXXXX \
  --country "AT,BE,BG,CY,CZ,DK,EE,FI,FR,DE,GR,HU,IE,IT,LV,LT,LU,MT,NL,PL,PT,RO,SK,SI,ES,SE,GB" \
  --family 4 \
  --ping-count 6 \
  --packet-interval 5000 \
  --sample-interval 300 \
  --start-delay 60 \
  --period 60 \
  --public \
  --anchor \
  100 2>&1 | tee get-measurements.log
```

#### 2.1.3 Checking progress:
If you want to keep an eye on the progress, I suggest you create a script like the following.
```bash
while [ 1 ]
do
  clear
  mysql -uXXXX -pXXXX atlas -e " \
     SELECT state, COUNT(*) AS total
     FROM measurements
     GROUP BY state" 2> /dev/null
  sleep 1
done
```
This shows you the count of the measurement in each state:
* `TO_BE_REQUESTED`: measurements that haven't yet been requested to Atlas via their APIs
* `REQUESTED`: measurements that have been requested
* `FETCHED`: measurements that have completed and their results have been been successfully fetched
* `FAILED`: measurements that have completed with an error, and their results are unavailable


### 2.2 Calculating paths
Once you have collected all the results, start this script to calculate all the paths and store results in database:
```bash
python3 calculate-paths.py
```

### 2.3 Exporting results
You can use this script to export your results to three files:
* `probes.csv` is a CSV table of the probes ID, IP address, IP address family and ASN
* `matrix.csv` is a CSV table representing a 2D matrix of the latency between each pair of probes.
* `notes.txt` with a bunch of useful metadata for your measurements
```bash
python3 export-results.py
```

### 2.4 Working with results
Once you get the results, you can study them with SQL queries like:
```SQL
-- Count for how many paths the 2-hop path has lower latency than the direct path:
SELECT count(*) FROM results WHERE h2 <= h1;
```

Keep in mind that `get-measurements.py` drops (!!) all tables without warning when invoked. So, you should save the results of previous measurements by renaming those tables, such as:
```bash
NEW_NAME='gb_1'
mysql -uatlas -pXXXX atlas -e "RENAME TABLE measurements TO measurements_${NEW_NAME};"
mysql -uatlas -pXXXX atlas -e "RENAME TABLE probes TO probes_${NEW_NAME};"
mysql -uatlas -pXXXX atlas -e "RENAME TABLE results TO results_${NEW_NAME};"
```

If you just want to export the results in CSV, for example to study them in Excel or in a Jupyter notebook with Pandas, do something like: 
```bash
NAME='gb_1'
echo "SELECT * FROM results_${1}" | mysql -B -uXXXX -pXXXX atlas | sed -e 's/\t/,/g' > ${1}.csv
```
