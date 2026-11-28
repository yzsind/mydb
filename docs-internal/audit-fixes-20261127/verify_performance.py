import pymysql,time,statistics,json
cs={}
for port in [14980,14994]:
 c=pymysql.connect(host='127.0.0.1',port=port,user='root',password='root',autocommit=True);cs[port]=c
 with c.cursor() as s:
  s.execute('SET GLOBAL mydb_replica_read_only=0');s.execute('CREATE DATABASE IF NOT EXISTS perf1127');s.execute('USE perf1127');s.execute('DROP TABLE IF EXISTS t');s.execute('CREATE TABLE t(id INT PRIMARY KEY,v INT)');s.executemany('INSERT INTO t VALUES(%s,%s)',[(i,i//8) for i in range(600)])
results={}
for label,sql,n in [('point','SELECT v FROM t WHERE id=42',1000),('update','UPDATE t SET v=v+1 WHERE id=42',500),('range_join','SELECT COUNT(*) FROM t a JOIN t b WHERE a.id<=b.id',1),('duplicate_join','SELECT COUNT(*) FROM t a JOIN t b ON a.id=b.v',1)]:
 samples={p:[] for p in cs}
 for _ in range(5):
  for port,c in cs.items():
   with c.cursor() as s:
    start=time.monotonic()
    for i in range(n):s.execute(sql);s.fetchall()
    samples[port].append((time.monotonic()-start)/n)
 results[label]=dict(samples=samples,ratio=statistics.median(samples[14994])/statistics.median(samples[14980]))
print(json.dumps(results))
