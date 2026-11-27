"""Exercise concurrent native bootstrap and semi-sync on owned local instances."""
import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import subprocess
import time
import pymysql

p=argparse.ArgumentParser();p.add_argument('--binary',required=True);p.add_argument('--root',required=True);p.add_argument('--port',type=int,default=14985);p.add_argument('--repl-port',type=int,default=13485);a=p.parse_args()
root=Path(a.root);root.mkdir(parents=True,exist_ok=True)
processes=[];logs=[]
def launch(name,port,extra):
 env=dict(os.environ,MYDB_REPL_AUTH_TOKEN='audit1127-token-for-native-repl',MYDB_MEMORY_TARGET='256',MYDB_REPL_SEMI_SYNC_TIMEOUT_MS='500',MYDB_REPL_MAX_BASE_BACKUPS='1',MYDB_REPL_BOOTSTRAP_MAX_ATTEMPTS='10',MYDB_REPL_BIND_HOST='127.0.0.1',**extra)
 log=open(root/(name+'.log'),'w');logs.append(log)
 proc=subprocess.Popen([a.binary,'--host','127.0.0.1','--port',str(port),'--data-dir',str(root/name)],stdout=log,stderr=log,env=env);processes.append(proc);return proc

def connect(port):return pymysql.connect(host='127.0.0.1',port=port,user='root',password='root',autocommit=True,connect_timeout=1,read_timeout=5)
def ready(port,timeout=60):
 for _ in range(int(timeout*10)):
  try:return connect(port)
  except Exception:time.sleep(.1)
 return None
try:
 launch('master',a.port,dict(MYDB_ROLE='master',MYDB_REPL_MODE='semi_sync',MYDB_REPL_PORT=str(a.repl_port)))
 c=ready(a.port);assert c,'master did not start'
 with c.cursor() as s:
  s.execute('CREATE DATABASE nativefix');s.execute('CREATE TABLE nativefix.bulk(id INT PRIMARY KEY,payload VARCHAR(500))')
  for start in range(0,60000,1000):s.executemany('INSERT INTO nativefix.bulk VALUES(%s,%s)',[(i,'v'*500) for i in range(start,start+1000)])
  s.execute('CREATE TABLE nativefix.probe(id INT PRIMARY KEY)')
 for i in range(4):launch('replica'+str(i),a.port+i+1,dict(MYDB_ROLE='replica',MYDB_REPL_MASTER_HOST='127.0.0.1',MYDB_REPL_MASTER_PORT=str(a.repl_port),MYDB_REPL_PORT=str(a.repl_port+i+1)))
 durations=[];errors=[]
 for i in range(30):
  t=time.monotonic()
  try:
   with c.cursor() as s:s.execute('INSERT INTO nativefix.probe VALUES(%s)',(i,))
  except Exception as e:errors.append(str(e))
  durations.append(time.monotonic()-t);time.sleep(.03)
 with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:replicas=list(pool.map(lambda port:ready(port,30),range(a.port+1,a.port+5)))
 results=[]
 for i,replica in enumerate(replicas):
  row=None
  if replica:
   with replica.cursor() as s:s.execute('SELECT COUNT(*) FROM nativefix.bulk');row=s.fetchone()
   replica.close()
  text=(root/('replica'+str(i)+'.log')).read_text();results.append(dict(replica=i,rows=row,capacity_rejections=text.count('concurrency limit reached'),retry='retry in' in text,exited=processes[i+1].poll()))
 print(json.dumps(dict(replicas=results,max_commit_seconds=max(durations),timeout_commits=sum(d>.45 for d in durations),write_errors=errors),ensure_ascii=False),flush=True)
 c.close()
finally:
 for proc in reversed(processes):
  if proc.poll() is None:proc.terminate()
 for proc in processes:
  try:proc.wait(timeout=10)
  except subprocess.TimeoutExpired:proc.kill();proc.wait()
 for log in logs:log.close()
