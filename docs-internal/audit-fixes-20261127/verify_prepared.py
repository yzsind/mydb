"""Verify wire prepared MVI keys against scans and repeated bound values."""
import argparse,json
import mysql.connector
p=argparse.ArgumentParser();p.add_argument('--port',type=int,required=True);p.add_argument('--password',default='root');a=p.parse_args()
c=mysql.connector.connect(host='127.0.0.1',port=a.port,user='root',password=a.password,autocommit=True,use_pure=True)
s=c.cursor();s.execute('SET GLOBAL mydb_replica_read_only=0');s.execute('CREATE DATABASE IF NOT EXISTS preparedfix');s.execute('USE preparedfix')
results=[]
for i,(target,doc,probes) in enumerate([
 ('CHAR(64)','["a","b"]',['a','b','missing']),
 ('DATE','["2024-01-01","2024-02-01"]',['2024-01-01','2024-02-01','2024-03-01']),
 ('DATETIME','["2024-01-01 12:00:00"]',['2024-01-01 12:00:00','2024-01-02 12:00:00']),
 ('TIME','["12:00:00","13:00:00"]',['12:00:00','13:00:00','14:00:00']),
 ('DECIMAL','[7,8]',[7,8,9]),
 ('DECIMAL(12,2)','[7.25,8.25]',[7.25,8.25,9.25]),
 ('SIGNED','[7,8]',[7,8,9]),
 ('UNSIGNED','[7,8]',[7,8,9]),
]):
 name='m'+str(i);s.execute('DROP TABLE IF EXISTS '+name);s.execute(f'CREATE TABLE {name}(id INT PRIMARY KEY,j JSON,KEY m((CAST(j AS {target} ARRAY))))');s.execute(f'INSERT INTO {name} VALUES(1,%s)',(doc,))
 prep=c.cursor(prepared=True)
 for value in probes*2:
  prep.execute(f'SELECT id FROM {name} FORCE INDEX(m) WHERE %s MEMBER OF(j)',(value,));indexed=prep.fetchall()
  s.execute(f'SELECT id FROM {name} IGNORE INDEX(m) WHERE %s MEMBER OF(j)',(value,));scan=s.fetchall()
  assert indexed==scan,(target,value,indexed,scan)
  results.append(dict(target=target,value=value,rows=indexed))
 prep.close()
# Prepared prefix comparisons must retain SQL comparison domains.
s.execute('DROP TABLE IF EXISTS cp');s.execute('CREATE TABLE cp(id INT PRIMARY KEY,vc VARCHAR(10),j JSON,KEY m(vc,(CAST(j AS UNSIGNED ARRAY))))');s.execute("INSERT INTO cp VALUES(1,'01','[7]'),(2,'1','[7]'),(3,'1.0','[7]')")
prep=c.cursor(prepared=True)
for value in [1,'1',1.0,'01']:
 prep.execute('SELECT id FROM cp WHERE vc=%s AND 7 MEMBER OF(j) ORDER BY id',(value,));indexed=prep.fetchall()
 s.execute('SELECT id FROM cp IGNORE INDEX(m) WHERE vc=%s AND 7 MEMBER OF(j) ORDER BY id',(value,));scan=s.fetchall();assert indexed==scan,(value,indexed,scan)
print(json.dumps(dict(probes=len(results)+4,results=results),default=str))
