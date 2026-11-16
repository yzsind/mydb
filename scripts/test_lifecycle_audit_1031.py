#!/usr/bin/env python3
"""Unix process/protocol regressions. Requires PyMySQL, cryptography and mysql-connector-python."""
import argparse, fcntl, json, os, pathlib, signal, socket, subprocess, tempfile, time
import pymysql

parser = argparse.ArgumentParser()
parser.add_argument('--binary', required=True)
parser.add_argument('--baseline', action='store_true')
a = parser.parse_args()
root = pathlib.Path(tempfile.mkdtemp(prefix='audit1031-', dir='/tmp/mydb'))
results = []

def record(name, fn):
    try:
        fn()
        results.append((name, 'PASS'))
    except Exception as exc:
        results.append((name, 'FAIL', str(exc)))
    print(results[-1], flush=True)

def port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0)); return s.getsockname()[1]

def env(data, extra=None):
    v = {k: v for k,v in os.environ.items() if not k.startswith('MYDB_')}
    v.update(MYDB_DATA_DIR=str(data), MYDB_TOKIO_WORKERS='2', MYDB_DATA_DIR_LOCK_WAIT_MS='0')
    v.update(extra or {}); return v

def conn(p, user='root'):
    return pymysql.connect(host='127.0.0.1', port=p, user=user, password='root' if user=='root' else 'p', database='test' if user=='root' else None, read_timeout=3, autocommit=True)

def start(name, extra=None, data=None):
    p = port(); data = data or root/name
    log = open(root/(name+'.log'), 'wb')
    proc = subprocess.Popen([a.binary,'--host','127.0.0.1','--port',str(p),'--ssl-mode','disabled'],env=env(data,extra),stdout=log,stderr=log)
    return proc,p,data

def stop(proc):
    if proc.poll() is None:
        proc.terminate()
        try: proc.wait(20)
        except subprocess.TimeoutExpired: proc.kill(); proc.wait(); raise
    assert proc.returncode == 0, 'unclean shutdown: '+str(proc.returncode)

def ready(proc,p):
    for _ in range(200):
        if proc.poll() is not None: raise AssertionError('server exited: '+str(proc.returncode))
        try: return conn(p)
        except (OSError,pymysql.Error): time.sleep(.05)
    raise AssertionError('server did not start')

proc,p,data = start('main')
try:
    admin = ready(proc,p)
    def quota():
        with admin.cursor() as c:
            c.execute("CREATE USER 'audit_quota'@'%' IDENTIFIED BY 'p'")
            c.execute('SET GLOBAL max_user_connections=2')
        sessions=[]
        try:
            sessions=[conn(p,'audit_quota'),conn(p,'audit_quota')]
            try:
                third=conn(p,'audit_quota'); third.close()
            except pymysql.Error as ex:
                assert ex.args[0] == 1203, ex
            else: raise AssertionError('third login accepted')
            with admin.cursor() as c: c.execute("ALTER USER 'audit_quota'@'%' WITH MAX_USER_CONNECTIONS 4")
            sessions += [conn(p,'audit_quota'),conn(p,'audit_quota')]
        finally:
            for s in sessions: s.close()
            with admin.cursor() as c: c.execute('SET GLOBAL max_user_connections=0')
    record('global/account connection quota',quota)
    def partial():
        victim=conn(p); sock=victim._sock
        try:
            body=b'\x03SELECT 42'; wire=len(body).to_bytes(3,'little')+b'\x00'+body
            sock.sendall(wire[:7]); time.sleep(.15)
            with admin.cursor() as c: c.execute('KILL QUERY '+str(victim.thread_id()))
            time.sleep(.15); sock.sendall(wire[7:]); victim._next_seq_id=1
            victim._read_query_result(False)
            assert victim._result.rows==((42,),),victim._result.rows
            with victim.cursor() as c: c.execute('SELECT 43'); assert c.fetchone()==(43,)
        finally: victim.close()
    record('partial command + KILL QUERY',partial)
    def charset():
        with admin.cursor() as c:
            c.execute('CREATE TABLE jc (j JSON)'); c.execute('INSERT INTO jc VALUES (%s)',('{"name":"数据库"}',))
        for cs in ['gbk','latin1']:
            s=conn(p)
            try:
                s.set_charset(cs)
                with s.cursor() as c:
                    c.execute('SELECT j FROM jc'); value=c.fetchone()[0]
                    assert ('数据库' if cs=='gbk' else '???') in value, value
                    c.execute('SELECT 1'); assert c.fetchone()==(1,)
            finally: s.close()
    record('JSON result charsets',charset)
    def timezone_index():
        with admin.cursor() as c:
            c.execute("CREATE TABLE tz (id INT PRIMARY KEY, ts TIMESTAMP, INDEX k ((CAST(ts AS DATE))))")
            c.execute("SET time_zone='+08:00'")
            c.execute("INSERT INTO tz VALUES (1,'2026-01-01 02:00:00')")
            c.execute("SET time_zone='+00:00'")
            for date in ['2025-12-31','2026-01-01']:
                c.execute("SELECT id FROM tz IGNORE INDEX(k) WHERE CAST(ts AS DATE)=%s", (date,)); expected=c.fetchall()
                c.execute("SELECT id FROM tz FORCE INDEX(k) WHERE CAST(ts AS DATE)=%s", (date,)); assert c.fetchall()==expected
    record('timezone dependent index matches full scan',timezone_index)
    def json_boundaries():
        with admin.cursor() as c:
            c.execute("SELECT JSON_TYPE('-0'),JSON_EXTRACT('-0','$'),JSON_TYPE('-0.0')")
            assert c.fetchone()==('INTEGER','0','DOUBLE')
            for depth in [100,101]:
                try:
                    c.execute('SELECT JSON_DEPTH(%s)', ('['*depth+'1'+']'*depth,))
                    assert depth==100 and c.fetchone()==(101,)
                except pymysql.Error as ex: assert depth==101 and ex.args[0]==3157,ex
    record('JSON negative zero and depth boundaries',json_boundaries)
    def prepared():
        import mysql.connector
        s=mysql.connector.connect(host='127.0.0.1',port=p,user='root',password='root',database='test',use_pure=True)
        try:
            c=s.cursor(); c.execute("CREATE TABLE pc (id INT PRIMARY KEY,s VARCHAR(50),KEY k ((LOWER(s))))"); c.execute("INSERT INTO pc VALUES (1,'Alice')"); s.commit(); c.close()
            c=s.cursor(prepared=True); c.execute("SELECT id FROM pc FORCE INDEX(k) WHERE LOWER(s)=%s",('ALICE',)); assert c.fetchall()==[(1,)]
            c.close()
            c=s.cursor(); c.execute('SET NAMES gbk'); c.close()
            c=s.cursor(prepared=True); c.execute('SELECT j FROM jc'); value=c.fetchone()[0]
            if isinstance(value,(bytes,bytearray)): value=value.decode('gbk')
            assert '数据库' in value,value
            c.close()
        finally: s.close()
    record('binary prepared collation and JSON charset',prepared)
    def wide_integer():
        number='12345678901234567890123456789012345678901234567890123456789012345'
        with admin.cursor() as c:
            c.execute("SELECT CAST(CAST(%s AS JSON) AS DECIMAL(65,0))",(number,))
            actual=str(c.fetchone()[0]); assert actual==number,(actual,number)
            c.execute('CREATE TABLE wi (id INT PRIMARY KEY,j JSON,KEY k ((CAST(j AS DECIMAL(65,0) ARRAY))))')
            c.execute('INSERT INTO wi VALUES (1,%s),(2,%s)',('['+number+']','['+'9'*65+']'))
            c.execute('SELECT id FROM wi FORCE INDEX(k) WHERE CAST(%s AS DECIMAL(65,0)) MEMBER OF(j)',(number,))
            assert c.fetchall()==((1,),)
    record('wide JSON integer and DECIMAL(65) multi-valued index',wide_integer)
    def hup():
        before=(root/'main.log').stat().st_size; proc.send_signal(signal.SIGHUP); time.sleep(.2)
        assert proc.poll() is None
        assert (root/'main.log').stat().st_size > before,'SIGHUP silently swallowed'
    record('observable SIGHUP',hup)
    admin.close()
finally: stop(proc)

def bind_fail():
    with socket.socket() as occupied:
        occupied.bind(('127.0.0.1',0)); occupied.listen()
        d=root/'occupied'
        r=subprocess.run([a.binary,'--host','127.0.0.1','--port',str(occupied.getsockname()[1]),'--ssl-mode','disabled'],env=env(d),capture_output=True,timeout=15)
        assert r.returncode != 0
        assert not (d/'schemas').exists(),'initialized database before binding'
        assert b'mydb listening on' not in r.stdout+r.stderr
record('occupied port has no initialized database',bind_fail)

def help_env():
    r=subprocess.run([a.binary,'--help'],env=env(root/'help',{'MYDB_PORT':'abc'}),capture_output=True,timeout=5)
    assert r.returncode==0,(r.returncode,r.stderr)
record('help ignores bad environment',help_env)

def workers():
    r=subprocess.run([a.binary,'--port','0'],env=env(root/'workers',{'MYDB_TOKIO_WORKERS':'1025'}),capture_output=True,timeout=5)
    assert r.returncode==2,(r.returncode,r.stderr)
    assert b'tokio_workers' in r.stderr or b'TOKIO_WORKERS' in r.stderr,r.stderr
record('worker bound validation',workers)

def http_cli():
    q=port(); d=root/'http'; log=open(root/'http.log','wb')
    cmd=[a.binary,'--host','127.0.0.1','--port',str(q),'--ssl-mode','disabled','--http-enabled','OFF','--http-local-config','', '--http-max-inflight','2','--http-max-queue','2','--http-memory-limit-mb','4']
    proc=subprocess.Popen(cmd,env=env(d),stdout=log,stderr=log)
    try: c=ready(proc,q); c.close()
    finally: stop(proc)
record('five HTTP CLI options',http_cli)

def bootstrap_lock():
    d=root/'bootstrap'
    extra={'MYDB_REPL_AUTH_TOKEN':'audit1031-local-test-token','MYDB_ROLE':'replica','MYDB_REPL_MASTER_HOST':'127.0.0.1','MYDB_REPL_MASTER_PORT':str(port()),'MYDB_REPL_BOOTSTRAP_MAX_ATTEMPTS':'0'}
    first,_,_=start('replica1',extra,d)
    second=None
    try:
        for _ in range(100):
            if 'seeding base backup' in (root/'replica1.log').read_text(): break
            if first.poll() is not None: raise AssertionError((root/'replica1.log').read_text())
            time.sleep(.05)
        else: raise AssertionError('replica did not enter bootstrap')
        lock_path=d/'system/mydb.lock'
        assert lock_path.exists(),'bootstrap has no lock file'
        with open(lock_path,'r+b') as lock:
            try: fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError: pass
            else: raise AssertionError('bootstrap directory is unlocked')
        second,_,_=start('replica2',extra,d)
        assert second.wait(5)!=0
        assert 'locked by another mydb process' in (root/'replica2.log').read_text()
    finally:
        # Bootstrap intentionally cannot reach its unavailable master; terminate
        # these isolated children without touching any user database.
        for proc in [first,second]:
            if proc and proc.poll() is None: proc.kill(); proc.wait()
record('directory locked throughout replica bootstrap',bootstrap_lock)

def native_replication():
    token='audit1031-local-test-token'
    repl_port=port()
    master,mp,_=start('native-master',{'MYDB_ROLE':'master','MYDB_REPL_PORT':str(repl_port),'MYDB_REPL_AUTH_TOKEN':token,'MYDB_LOG_BIN':'ON','MYDB_BINLOG_RETENTION_HOURS':'1'})
    replica=None
    m=r=None
    try:
        m=ready(master,mp)
        with m.cursor() as c:
            c.execute("CREATE TABLE nr (id INT PRIMARY KEY,s VARCHAR(50),a DECIMAL(12,2),KEY k ((CONCAT(s,''))),KEY n ((a+0.01)))")
            c.execute("INSERT INTO nr VALUES (1,'Alice',2.40)")
        extra={'MYDB_ROLE':'replica','MYDB_REPL_MASTER_HOST':'127.0.0.1','MYDB_REPL_MASTER_PORT':str(repl_port),'MYDB_REPL_AUTH_TOKEN':token}
        replica,rp,rd=start('native-replica',extra)
        r=ready(replica,rp)
        def expect(text):
            for _ in range(200):
                with r.cursor() as c:
                    c.execute("SELECT id FROM nr FORCE INDEX(k) WHERE CONCAT(s,'')=%s",(text,))
                    if c.fetchall()==((1,),): return
                time.sleep(.05)
            raise AssertionError('replication did not reach '+text)
        expect('ALICE')
        with m.cursor() as c: c.execute("UPDATE nr SET s='Bob',a=3.50 WHERE id=1")
        expect('BOB')
        r.close(); r=None
        stop(replica)
        replica,rp,_=start('native-replica-reopen',extra,rd)
        r=ready(replica,rp)
        expect('BOB')
        with r.cursor() as c:
            c.execute('SELECT id FROM nr FORCE INDEX(n) WHERE a+0.01>3.0'); assert c.fetchall()==((1,),)
    finally:
        if r: r.close()
        if m: m.close()
        try:
            if replica: stop(replica)
        finally: stop(master)
record('native bootstrap, replay, indexed reads and clean replica restart',native_replication)

def cli_conventions():
    import re
    log=open(root/'dynamic-port.log','wb')
    proc=subprocess.Popen([a.binary,'--host','127.0.0.1','--port','12345','--port','0','--ssl-mode','disabled'],env=env(root/'dynamic-port'),stdout=log,stderr=log)
    try:
        for _ in range(200):
            text=(root/'dynamic-port.log').read_text()
            match=re.search(r'mydb listening on 127\.0\.0\.1:(\d+)',text)
            if match: break
            if proc.poll() is not None: raise AssertionError(text)
            time.sleep(.05)
        else: raise AssertionError('no listening address')
        actual=int(match.group(1)); assert actual not in [0,12345],actual
        c=ready(proc,actual); c.close()
    finally: stop(proc)
    r=subprocess.run([a.binary,'--version'],env=env(root/'version',{'MYDB_PORT':'abc'}),capture_output=True,timeout=5)
    assert r.returncode==0 and b'mydb' in r.stdout,r.stderr
record('dynamic port, duplicate precedence and version',cli_conventions)

(root/'results.json').write_text(json.dumps(results,ensure_ascii=False,indent=2))
print('artifacts:',root)
if not a.baseline and any(x[1]=='FAIL' for x in results): raise SystemExit(1)
