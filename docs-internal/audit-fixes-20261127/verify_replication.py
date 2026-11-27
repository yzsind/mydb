"""Verify ROW/FULL padding, live coordinates and a 72 MB update on dedicated peers."""
import pymysql,time,json,sys
src=pymysql.connect(host='127.0.0.1',port=14981,user='root',autocommit=True)
dst=pymysql.connect(host='127.0.0.1',port=14992,user='root',password='root',autocommit=True)
def q(c,s):
 with c.cursor(pymysql.cursors.DictCursor) as x:x.execute(s);return x.fetchall()
def status():return q(dst,"SHOW REPLICA STATUS FOR CHANNEL 'audit'")[0]
def report(label):
 s=status();print(json.dumps(dict(label=label,**{k:v for k,v in s.items() if k in ('Source_Log_File','Read_Source_Log_Pos','Relay_Source_Log_File','Exec_Source_Log_Pos','Last_IO_Errno','Last_IO_Error','Last_SQL_Errno','Last_SQL_Error','Replica_IO_Running','Replica_SQL_Running')})),flush=True)
def wait(sql,expected,timeout=30):
 for _ in range(int(timeout*10)):
  try:
   if q(dst,sql)==expected:return True
  except Exception:pass
  time.sleep(.1)
 return False
q(src,"CREATE USER IF NOT EXISTS 'audit'@'127.0.0.1' IDENTIFIED WITH mysql_native_password BY 'audit'")
q(src,"GRANT REPLICATION SLAVE,REPLICATION CLIENT ON *.* TO 'audit'@'127.0.0.1'")
q(src,'DROP DATABASE IF EXISTS replfix1127');q(src,'FLUSH BINARY LOGS');coord=q(src,'SHOW MASTER STATUS')[0]
q(dst,f"CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1',SOURCE_PORT=14981,SOURCE_USER='audit',SOURCE_PASSWORD='audit',SOURCE_LOG_FILE='{coord['File']}',SOURCE_LOG_POS={coord['Position']} FOR CHANNEL 'audit'")
q(dst,"START REPLICA FOR CHANNEL 'audit'")
q(src,'CREATE DATABASE replfix1127');q(src,'CREATE TABLE replfix1127.kn(a CHAR(5),b BINARY(4),c INT)')
q(src,"INSERT INTO replfix1127.kn VALUES('abc',x'41420000',1),('abc',x'00FF0A0D',2)")
q(src,"UPDATE replfix1127.kn SET c=10 WHERE a='abc'")
assert wait('SELECT SUM(c) AS total FROM replfix1127.kn',[{'total':20}],30),status();report('keyless-after-update');q(src,"DELETE FROM replfix1127.kn WHERE b=x'41420000'");assert wait('SELECT COUNT(*) AS n FROM replfix1127.kn',[{'n':1}],30),status()
q(dst,"STOP REPLICA FOR CHANNEL 'audit'");before=status();q(dst,"CHANGE REPLICATION SOURCE TO SOURCE_CONNECT_RETRY=1 FOR CHANNEL 'audit'");report('change-no-coordinate');print('previous-exec',before['Exec_Source_Log_Pos'],flush=True)
# Continue from a new source file after checking incremental coordinate changes.
q(src,'FLUSH BINARY LOGS');coord=q(src,'SHOW MASTER STATUS')[0]
q(dst,f"CHANGE REPLICATION SOURCE TO SOURCE_LOG_FILE='{coord['File']}',SOURCE_LOG_POS={coord['Position']} FOR CHANNEL 'audit'")
q(dst,"START REPLICA FOR CHANNEL 'audit'")
q(src,'CREATE TABLE replfix1127.big(id INT PRIMARY KEY,v LONGTEXT)');q(src,"INSERT INTO replfix1127.big VALUES(1,'x')")
q(src,"UPDATE replfix1127.big SET v=REPEAT('A',36000000) WHERE id=1")
wait('SELECT LENGTH(v) AS n FROM replfix1127.big',[{'n':36000000}],60)
q(src,"UPDATE replfix1127.big SET v=REPEAT('B',36000000) WHERE id=1")
assert wait("SELECT LEFT(v,1) AS c,LENGTH(v) AS n FROM replfix1127.big",[{'c':'B','n':36000000}],120),status();report('large-update-72MB');print('rows',q(dst,'SELECT HEX(a),HEX(b),c FROM replfix1127.kn'),flush=True)
q(dst,"STOP REPLICA FOR CHANNEL 'audit'")
