"""Run twice on a dedicated instance started with MYDB_MEMORY_TARGET=256 MYDB_QUERY_MEM_LIMIT_MB=32."""
import pymysql,time,json,concurrent.futures
c=pymysql.connect(host='127.0.0.1',port=14994,user='root',password='root',autocommit=True,read_timeout=120)
s=c.cursor();s.execute('CREATE DATABASE IF NOT EXISTS memfix');s.execute('USE memfix');s.execute('DROP TABLE IF EXISTS m');s.execute('CREATE TABLE m(id INT PRIMARY KEY,v INT)')
s.executemany('INSERT INTO m VALUES(%s,%s)',[(i,i) for i in range(8000)])
for i in range(2):
 t=time.monotonic()
 try:s.execute('SELECT COUNT(*) FROM m a JOIN m b WHERE a.id<=b.id');print(i,'result',s.fetchall(),time.monotonic()-t,flush=True)
 except Exception as e:print(i,'error',str(e),time.monotonic()-t,flush=True)
 for sql in ['SELECT 1','BEGIN','UPDATE m SET v=v+1 WHERE id=1','ROLLBACK','SELECT v FROM m WHERE id=1']:
  try:s.execute(sql);print(sql,s.fetchall(),flush=True)
  except Exception as e:print(sql,str(e),flush=True)
 time.sleep(.2)
