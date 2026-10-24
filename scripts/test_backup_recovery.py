#!/usr/bin/env python3
"""Local acceptance for the unified backup protocol; keeps evidence in /tmp/mydb.
Build: cargo build --features tools --bin mydb --bin mydb-backup
Run: python scripts/test_backup_recovery.py --profile debug --rows 1000 --pairs 5
Requires mysql-connector-python. Does not flush the operating system cache.
"""
import argparse
import datetime
import ctypes
import sys
import resource
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import statistics
import subprocess
import tempfile
import threading
import time
import struct
import zlib
import mysql.connector

ROOT = Path(__file__).resolve().parents[1]
PROCESSES = []
CLI_IO = []

def process_io(pid):
    """Kernel disk I/O counters; zero reads can reflect the warm OS cache."""
    if sys.platform != 'darwin':
        return {}
    class Usage(ctypes.Structure):
        _fields_ = [('uuid', ctypes.c_uint8 * 16)] + [(name, ctypes.c_uint64) for name in
            ['user','system','idle','interrupt','pageins','wired','resident','footprint',
             'start','exit','child_user','child_system','child_idle','child_interrupt',
             'child_pageins','child_elapsed','read_bytes','written_bytes']]
    usage=Usage(); library=ctypes.CDLL('/usr/lib/libproc.dylib')
    if library.proc_pid_rusage(pid,2,ctypes.byref(usage))!=0:
        raise OSError('proc_pid_rusage failed')
    return {'kernel_read_bytes':usage.read_bytes,'kernel_written_bytes':usage.written_bytes,
            'resident_bytes':usage.resident,'physical_footprint_bytes':usage.footprint}


def sql(c, text, prepared=False):
    cur = c.cursor(prepared=prepared)
    try:
        cur.execute(text)
        return cur.fetchall() if cur.with_rows else []
    finally:
        cur.close()

def connect(port):
    c=mysql.connector.connect(host='127.0.0.1', port=port, user='root', password='root',
                              database='test', autocommit=True, use_pure=True,
                              connection_timeout=2)
    sql(c,"SET time_zone='+00:00'")
    return c

def start(binary, artifact, data=None, repository=None, base=None, key=None):
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0)); port = s.getsockname()[1]
    env = {k: v for k, v in os.environ.items() if not k.startswith('MYDB_')}
    env.update(TMPDIR='/tmp/mydb', MYDB_ROOT_PASSWORD='root', MYDB_DURABILITY='strict',
               MYDB_MEMORY_TARGET='512M', MYDB_DATA_DIR_LOCK_WAIT_MS='0',
               MYDB_SORT_RUN_BYTES='1048576', MYDB_TEMP_DISK_RESERVE_PCT='1',
               MYDB_HNSW_BRUTE_THRESHOLD='0', MYDB_HNSW_BRUTE_FILTER_ROWS='0')
    args = [str(binary), '--host', '127.0.0.1', '--port', str(port), '--ssl-mode', 'auto']
    if repository:
        args += ['--backup-repository', str(repository), '--base', base,
                 '--runtime-dir', str(artifact.parent / (artifact.stem + '-runtime'))]
    else:
        env['MYDB_DATA_DIR'] = str(data)
    if key:
        env['MYDB_BACKUP_KEY_FILE'] = str(key)
    began = time.perf_counter()
    with artifact.open('wb') as output:
        p = subprocess.Popen(args, env=env, stdout=output, stderr=output)
    PROCESSES.append(p)
    until = time.monotonic() + 120
    while time.monotonic() < until:
        if p.poll() is not None:
            raise RuntimeError(f'{artifact}: server exited\n{artifact.read_text()[-6000:]}')
        try:
            c = connect(port); sql(c, 'SELECT 1')
            p.startup_io = process_io(p.pid)
            return p, c, port, time.perf_counter() - began
        except mysql.connector.Error:
            time.sleep(.025)
    raise TimeoutError(str(artifact))

def stop(p, c):
    try:
        sql(c, 'SHUTDOWN')
    except mysql.connector.Error:
        pass
    c.close(); p.wait(timeout=60)

def wait(c, request):
    if not request:
        return None
    job_id = request[0][0]
    until = time.monotonic() + 180
    while time.monotonic() < until:
        rows = sql(c, 'SHOW MYDB BACKUP JOBS')
        row = next(x for x in rows if x[0] == job_id)
        if row[2] == 'COMPLETED':
            value=dict(zip(['id','kind','state','phase','base','requested_lsn','requested_time',
                             'durable_lsn','covered_time','generation','bytes','error'], row))
            for name in ['requested_lsn','requested_time','durable_lsn','covered_time','generation','bytes']:
                value[name]=int(value[name])
            return value
        if row[2] != 'RUNNING':
            raise AssertionError(row)
        time.sleep(.025)
    raise TimeoutError(job_id)

def action(c, text):
    t = time.perf_counter(); result = wait(c, sql(c, text))
    return result, time.perf_counter() - t

def status(c):
    return dict(sql(c, 'SHOW MYDB BACKUP STATUS'))

def tree(path):
    result = {}
    for p in sorted(path.rglob('*')):
        if p.is_file():
            h = hashlib.sha256()
            with p.open('rb') as f:
                for chunk in iter(lambda: f.read(4*1024*1024), b''):
                    h.update(chunk)
            result[str(p.relative_to(path))] = [p.stat().st_size, h.hexdigest()]
    return result

def digest(rows):
    return hashlib.sha256(repr(rows).encode()).hexdigest()

def rejected(c, text):
    try:
        sql(c, text)
    except mysql.connector.Error:
        return
    raise AssertionError(f'accepted unsupported/write SQL: {text}')

def source_commits_after(data_dir, end_lsn):
    """Independent bounded decoder for the protected source WAL suffix."""
    commits={}; spans=[]
    for path in sorted((data_dir/'system'/'wal').glob('wal-*')):
        if not (path.name.endswith('.log') or path.name.endswith('.log.pending')):
            continue
        with path.open('rb') as f:
            header=f.read(40)
            if len(header)!=40 or header[:8]!=b'MYDBWAL1':
                continue
            assert struct.unpack_from('<I',header,8)[0]==34
            while True:
                frame=f.read(48)
                if not frame or frame[:4]==b'\0'*4:
                    break
                if len(frame)!=48:
                    break
                magic,size,lsn,previous,txn,kind=struct.unpack_from('<IIQQQH',frame)
                assert magic==0x4d57414c
                payload=f.read(size)
                if len(payload)!=size:
                    break
                assert zlib.crc32(frame[:44]+payload)==struct.unpack_from('<I',frame,44)[0]
                source_time=struct.unpack_from('<Q',frame,36)[0]
                count=1; decision=kind==2
                if kind==24:
                    count=struct.unpack_from('<I',payload)[0]; offset=4; decision=False
                    for _ in range(count):
                        ty,length=struct.unpack_from('<HI',payload,offset);offset+=6
                        decision=decision or ty==2;offset+=length
                    assert offset==len(payload)
                if decision:commits[txn]=(lsn,source_time)
                if lsn>end_lsn:
                    spans.append((lsn-count+1,lsn))
    expected=end_lsn+1
    for first,last in sorted(spans):
        assert first==expected,(first,last,expected)
        expected=last+1
    return commits

def latency_summary(samples, elapsed):
    ordered=sorted(samples)
    if not samples:return {'transactions':0,'seconds':elapsed}
    return {'transactions':len(samples),'seconds':elapsed,'tps':len(samples)/elapsed,
            'p95_ms':ordered[int((len(samples)-1)*.95)]*1000,'p99_ms':ordered[int((len(samples)-1)*.99)]*1000}

def invariant(c):
    records=sql(c,'SELECT id, token FROM journal ORDER BY id')
    assert records==[(n,f'committed-{n}') for n in range(1,len(records)+1)]
    assert sql(c,'SELECT id,v FROM balances ORDER BY id')==[(1,len(records)),(2,-len(records))]
    return len(records)

def bench(c, count=3000):
    samples=[]; t=time.perf_counter()
    for i in range(count):
        p=time.perf_counter(); sql(c, f'UPDATE probe SET v=v+1 WHERE id={1+i%2}')
        samples.append(time.perf_counter()-p)
    elapsed=time.perf_counter()-t; ordered=sorted(samples)
    return {'transactions':count,'seconds':elapsed,'tps':count/elapsed,
            'p95_ms':ordered[int((count-1)*.95)]*1000,'p99_ms':ordered[int((count-1)*.99)]*1000}

def measure_while(c, operation):
    active=threading.Event();active.set();result={};samples=[];retries=0
    def background():
        began=time.perf_counter()
        try:result['value']=operation()
        except Exception as error:result['error']=str(error)
        finally:result['seconds']=time.perf_counter()-began;active.clear()
    worker=threading.Thread(target=background);worker.start();began=time.perf_counter();n=0
    while active.is_set():
        tick=time.perf_counter()
        try:sql(c,f'UPDATE probe SET v=v+1 WHERE id={1+n%2}')
        except mysql.connector.Error as error:
            if error.errno==1205:retries+=1;continue
            raise
        samples.append(time.perf_counter()-tick);n+=1
    elapsed=time.perf_counter()-began;worker.join();assert 'error' not in result,result
    metric=latency_summary(samples,elapsed);metric['admission_retries']=retries;metric['operation_seconds']=result['seconds'];return metric,result['value']

def cli_run(cli, *args):
    before=resource.getrusage(resource.RUSAGE_CHILDREN);began=time.perf_counter()
    result=subprocess.run([str(cli), *map(str,args)], check=True, capture_output=True, text=True)
    after=resource.getrusage(resource.RUSAGE_CHILDREN)
    CLI_IO.append({'operation':str(args[0]),'seconds':time.perf_counter()-began,
                   'input_blocks':after.ru_inblock-before.ru_inblock,
                   'output_blocks':after.ru_oublock-before.ru_oublock})
    return result.stdout

def scenario(binary, cli, root, rows, pairs, encrypted):
    work=root/('encrypted' if encrypted else 'plain'); work.mkdir()
    repository=work/'repository'; key=None
    if encrypted:
        key=work/'keys'; key.write_text('42'*32); key.chmod(0o600)
        cli_run(cli,'init','--repository',repository,'--wal-key-id','acceptance','--key-file',key)
    key_args=['--key-file',key] if key else []
    p,c,port,startup=start(binary,work/'source.log',data=work/'source',key=key)
    sql(c,'SET GLOBAL mydb_undo_retention_ms=600000')
    sql(c,'CREATE TABLE t(id BIGINT PRIMARY KEY,k INT,v VARCHAR(80),j JSON,payload LONGBLOB,INDEX ik(k))')
    for begin in range(1,rows+1,100):
        values=','.join(f"({i},{i%17},'row-{i}',JSON_OBJECT('n',{i}),REPEAT(CHAR(65+{i%26}),128))" for i in range(begin,min(begin+100,rows+1)))
        sql(c,'INSERT INTO t VALUES '+values)
    for q in ['CREATE TABLE probe(id INT PRIMARY KEY,v BIGINT)','INSERT INTO probe VALUES(1,0),(2,0)',
              'CREATE TABLE balances(id INT PRIMARY KEY,v BIGINT)',
              'INSERT INTO balances VALUES(1,0),(2,0)',
              'CREATE TABLE journal(id INT PRIMARY KEY,token VARCHAR(40))',
              'CREATE TABLE hist(id INT PRIMARY KEY,v VARCHAR(20))',
              "INSERT INTO hist VALUES(1,'before')",
              'CREATE TABLE parts(id INT PRIMARY KEY,v INT) PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)',
              'INSERT INTO parts VALUES(1,11),(20,22)',
              'CREATE TABLE geo(id INT PRIMARY KEY,g GEOMETRY)',
              "INSERT INTO geo VALUES(1,ST_GeomFromText('POINT(1 2)'))",
              'CREATE TABLE docs(id INT PRIMARY KEY,body TEXT,FULLTEXT KEY ft(body))',
              "INSERT INTO docs VALUES(1,'backup database'),(2,'database history')",
              'CREATE TABLE vecs(id INT PRIMARY KEY,emb VECTOR(3) NOT NULL)',
              "INSERT INTO vecs VALUES(1,'[0,0,0]'),(2,'[1,0,0]'),(3,'[2,1,0]')",
              'CREATE INDEX vi ON vecs(emb) USING HNSW',
              'CREATE TABLE ivfs(id INT PRIMARY KEY,emb VECTOR(3) NOT NULL)',
              "INSERT INTO ivfs VALUES(1,'[0,0,0]'),(2,'[1,0,0]'),(3,'[2,1,0]')",
              "CREATE INDEX ii ON ivfs(emb) USING IVF_SQ8 COMMENT 'NLIST=1 NPROBE=1'",
              'CREATE SEQUENCE backup_seq','CREATE TABLE ai(id BIGINT AUTO_INCREMENT PRIMARY KEY,v INT)',
              'INSERT INTO ai(v) VALUES(10)']:
        sql(c,q)
    queries=['SELECT id,k,v,CAST(j AS CHAR),SHA2(payload,256) FROM t ORDER BY id',
             'SELECT id FROM t FORCE INDEX(ik) WHERE k BETWEEN 3 AND 6 ORDER BY id',
             "SELECT id FROM docs WHERE MATCH(body) AGAINST('database') ORDER BY id",
             "SELECT id FROM vecs ORDER BY DISTANCE(emb,'[0,0,0]') LIMIT 2",
             "SELECT id FROM ivfs ORDER BY DISTANCE(emb,'[0,0,0]') LIMIT 2",
             'SELECT id,v FROM ai ORDER BY id',
             'SELECT id,v FROM parts ORDER BY id',
             'SELECT id,ST_AsText(g) FROM geo ORDER BY id',
             'SELECT id,v FROM hist ORDER BY id']
    model=[sql(c,q) for q in queries]
    xa=connect(port)
    for q in ["XA START 'acceptance-xa'","UPDATE t SET v='xa-committed',payload=REPEAT('xa',12000),j=JSON_OBJECT('pending',1) WHERE id=5",f"INSERT INTO t VALUES({rows+1},5,'xa-insert',JSON_OBJECT('xa',1),'pending')","DELETE FROM t WHERE id=6","UPDATE docs SET body='prepared content' WHERE id=1","UPDATE vecs SET emb='[100000,0,0]' WHERE id=2","UPDATE ivfs SET emb='[100000,0,0]' WHERE id=2","XA END 'acceptance-xa'","XA PREPARE 'acceptance-xa'"]:
        sql(xa,q)
    xa.close()
    assert [sql(c,q) for q in queries]==model
    metrics={'source_start_seconds':startup,'encryption':encrypted,'rows':rows,'checks':[], 'pairs':[]}
    # A standalone full has no long-term pin or automatic WAL worker.
    standalone=work/'standalone'; done,elapsed=action(c,f"BACKUP DATABASE TO 'file://{standalone}'")
    assert status(c)['Effective_Policy']=='DISABLED'
    rejected(c,f"BACKUP WAL TO 'file://{standalone}'")
    cli_run(cli,'verify','--repository',standalone)
    metrics['standalone_full_seconds']=elapsed
    # First enabled full must hand E+1 retention to the new stream.
    sql(c,f"ALTER MYDB BACKUP TO 'file://{repository}' SET WAL_ARCHIVE=MANUAL")
    assert status(c)['Lifecycle']=='WAITING_BASE'
    writer_stop=threading.Event();writer_errors=[];writer_samples=[];writer_ledger=[]
    def writer():
        w=connect(port);n=0
        try:
            while not writer_stop.is_set():
                n+=1;began=time.perf_counter()
                try:
                    for q in ['BEGIN','UPDATE balances SET v=v+1 WHERE id=1','UPDATE balances SET v=v-1 WHERE id=2',f"INSERT INTO journal VALUES({n},'committed-{n}')"]:
                        sql(w,q)
                    txn=int(sql(w,'SELECT trx_id FROM information_schema.innodb_trx WHERE trx_mysql_thread_id=CONNECTION_ID()')[0][0])
                    sql(w,'COMMIT');writer_ledger.append((n,txn))
                    writer_samples.append(time.perf_counter()-began)
                except mysql.connector.Error as error:
                    sql(w,'ROLLBACK')
                    if error.errno==1205:
                        n-=1
                        continue
                    raise
        except Exception as e:writer_errors.append(str(e))
        finally:w.close()
    worker=threading.Thread(target=writer);writer_began=time.perf_counter();worker.start()
    try:
        done,elapsed=action(c,f"BACKUP DATABASE TO 'file://{repository}'"); base=done['base']
    finally:
        writer_stop.set();worker.join(timeout=30)
    assert not worker.is_alive() and not writer_errors,writer_errors
    metrics['source_during_capture_and_seal']=latency_summary(writer_samples,time.perf_counter()-writer_began)
    journal_final=invariant(c)
    data=repository/'bases'/base/'data'; before=tree(data)
    marker=json.loads((data/'mydb-snapshot.json').read_text())
    metrics.update(base=base,source_end_lsn=marker['manifest']['end_lsn'],
                   base_full_seconds=elapsed,final_barrier_ms=marker['manifest']['final_barrier_ms'],
                   base_bytes=sum(x[0] for x in before.values()),
                   base_allocated_bytes=sum(p.stat().st_blocks*512 for p in data.rglob('*') if p.is_file()))
    assert len(list((repository/'bases').glob('*/data')))==1
    assert not (repository/'chunks').exists()
    original_stream=status(c)['Stream_ID']
    suffix_commits=source_commits_after(work/'source',marker['manifest']['end_lsn'])
    assert all(txn in suffix_commits for _,txn in writer_ledger), 'commit evidence must survive until independently classified'
    expected_base_journal=sum(suffix_commits[txn][0]<=marker['manifest']['end_lsn'] for _,txn in writer_ledger)
    assert [n for n,txn in writer_ledger if suffix_commits[txn][0]<=marker['manifest']['end_lsn']]==list(range(1,expected_base_journal+1))
    (work/'commit-ledger.json').write_text(json.dumps([{'sequence':n,'txn':txn,'commit_lsn':suffix_commits[txn][0], 'source_time':suffix_commits[txn][1],
        'expected_digest':digest([(n,f'committed-{n}'),(1,n),(2,-n)])} for n,txn in writer_ledger],indent=2))
    # Capture an adjacent nontransactional sequence boundary (exact E+1 is
    # separately controlled by the storage-level boundary callback test).
    seq=sql(c,"SELECT NEXTVAL('backup_seq')")[0][0]
    first,_=action(c,f"BACKUP WAL TO 'file://{repository}'")
    history_time=sql(c,'SELECT CURRENT_TIMESTAMP(6)')[0][0]
    sql(c,"UPDATE hist SET v='after' WHERE id=1")
    sql(c,"XA COMMIT 'acceptance-xa'")
    sql(c,'ALTER TABLE t ADD COLUMN extra INT DEFAULT 7')
    sql(c,'CREATE TABLE created_after(id INT PRIMARY KEY,v INT)')
    sql(c,'INSERT INTO created_after VALUES(1,123)')
    sql(c,'RENAME TABLE created_after TO renamed_after')
    sql(c,'ALTER TABLE parts TRUNCATE PARTITION p0')
    sql(c,'INSERT INTO parts VALUES(2,33)')
    sql(c,"UPDATE geo SET g=ST_GeomFromText('POINT(3 4)') WHERE id=1")
    sql(c,"UPDATE t SET v='updated',payload=REPEAT('lob',10000) WHERE id=1")
    sql(c,'DELETE FROM t WHERE id=2'); sql(c,"INSERT INTO ai(v) VALUES(20)")
    sql(c,"UPDATE docs SET body='new backup content' WHERE id=2")
    sql(c,"UPDATE vecs SET emb='[0.1,0,0]' WHERE id=3")
    sql(c,"UPDATE ivfs SET emb='[0.1,0,0]' WHERE id=3")
    expected=[sql(c,q) for q in queries]
    middle,_=action(c,f"BACKUP WAL TO 'file://{repository}'")
    sql(c,"UPDATE t SET v='latest' WHERE id=1")
    # An unfinished transaction at F is a restore loser; roll back only there.
    loser=connect(port); sql(loser,'BEGIN'); sql(loser,"UPDATE t SET v='loser' WHERE id=3")
    latest,_=action(c,f"BACKUP WAL TO 'file://{repository}'")
    sql(loser,'ROLLBACK'); loser.close()
    expected_latest=[sql(c,q) for q in queries]
    # Source restart/checkpoint cannot discard the manual backlog.
    sql(c,"UPDATE t SET v='after-restart' WHERE id=4"); stop(p,c)
    p,c,port,_=start(binary,work/'restarted.log',data=work/'source',key=key)
    assert int(sql(c,"SELECT @@global.mydb_undo_retention_ms")[0][0])==600000
    assert sql(c,f"SELECT v FROM hist FOR SYSTEM_TIME AS OF '{history_time}' WHERE id=1")==[('before',)]
    action(c,f"BACKUP WAL TO 'file://{repository}'")
    query_p,query_c,_,query_start=start(binary,work/'direct.log',repository=repository,base=base)
    query2_p,query2_c,_,_=start(binary,work/'direct-2.log',repository=repository,base=base)
    assert invariant(query_c)==expected_base_journal
    assert invariant(query2_c)==invariant(query_c)
    assert [sql(query_c,q,prepared=(i==0)) for i,q in enumerate(queries)]==model
    rejected(query_c,"INSERT INTO ai(v) VALUES(999)")
    metrics['direct_start_seconds']=query_start
    metrics['direct_start_io']=query_p.startup_io
    reader_errors=[]
    def read_repeatedly():
        try:
            for _ in range(3):assert [sql(query2_c,q) for q in queries]==model
        except Exception as error:reader_errors.append(str(error))
    reader=threading.Thread(target=read_repeatedly);reader.start()
    metrics['source_with_direct_queries']=bench(c);reader.join();assert not reader_errors,reader_errors
    metrics['direct_runtime_files']={str(p.relative_to(work/'direct-runtime')):p.stat().st_size for p in (work/'direct-runtime').rglob('*') if p.is_file()}
    assert not any(p.name in ['objects','raw','prepared'] for p in (work/'direct-runtime').rglob('*'))
    for name,target,answer in [('middle',['--target-lsn',middle['requested_lsn']],expected),
                               ('time',['--target-time',datetime.datetime.fromtimestamp(middle['requested_time']/1e6,datetime.timezone.utc).isoformat()],expected),
                               ('loser',['--target-lsn',latest['requested_lsn']],expected_latest),
                               ('nearest',['--target-lsn',first['requested_lsn']],model)]:
        dest=work/name; t=time.perf_counter()
        result={}
        def restore_target():
            began=time.perf_counter()
            try:result['report']=json.loads(cli_run(cli,'restore','--repository',repository,'--base',base,'--data-dir',dest,*target,*key_args))
            except Exception as e:result['error']=str(e)
            result['seconds']=time.perf_counter()-began
        rt=threading.Thread(target=restore_target);rt.start()
        if name=='middle':metrics['source_during_restore']=bench(c)
        rt.join();assert 'error' not in result,result
        report=result['report'];restore_seconds=result['seconds']
        rp,rc,_,rs=start(binary,work/(name+'.log'),data=dest)
        actual=[sql(rc,q) for q in queries]; assert actual==answer,(name,[digest(x) for x in actual],[digest(x) for x in answer])
        assert invariant(rc)==journal_final
        assert sql(rc,f"SELECT v FROM hist FOR SYSTEM_TIME AS OF '{history_time}' WHERE id=1")==[('before',)]
        if name!='nearest':
            try:sql(rc,f"SELECT v FROM t FOR SYSTEM_TIME AS OF '{history_time}' WHERE id=1")
            except mysql.connector.Error as error:assert error.errno==50041
            else:raise AssertionError('historical read must respect the restored DDL structural epoch')
        if name!='nearest':
            assert sql(rc,'SELECT v FROM renamed_after')==[(123,)]
            assert sql(rc,'SELECT extra FROM t WHERE id=1')==[(7,)]
        stop(rp,rc);metrics[name]={'restore_seconds':restore_seconds,'start_seconds':rs,'report':report,'digests':[digest(x) for x in actual]}
    assert tree(data)==before
    # While a direct reader is alive, deletion must fail without changing roots.
    blocked=subprocess.run([str(cli),'prune','--repository',str(repository),'--apply',*map(str,key_args)],capture_output=True)
    assert blocked.returncode!=0
    stop(query_p,query_c);assert tree(data)==before
    blocked=subprocess.run([str(cli),'prune','--repository',str(repository),'--apply',*map(str,key_args)],capture_output=True);assert blocked.returncode!=0
    stop(query2_p,query2_c);assert tree(data)==before
    # At least five alternating pairs compare disabled source traffic to
    # active archiving; every enabled period establishes a fresh stream.
    for pair in range(pairs):
        order=['disabled','manual'] if pair%2==0 else ['manual','disabled']; measurements={}
        for mode in order:
            st=status(c)
            if mode=='disabled' and st['Effective_Policy']!='DISABLED':
                action(c,f"ALTER MYDB BACKUP TO 'file://{repository}' SET WAL_ARCHIVE=DISABLED")
            if mode=='manual' and st['Effective_Policy']=='DISABLED':
                sql(c,f"ALTER MYDB BACKUP TO 'file://{repository}' SET WAL_ARCHIVE=MANUAL")
                action(c,f"BACKUP DATABASE TO 'file://{repository}'")
            measurements[mode]=bench(c)
            if mode=='manual':
                r,t=action(c,f"BACKUP WAL TO 'file://{repository}'");measurements[mode].update(archive_seconds=t,archive_raw_bytes=r['bytes'])
        control=connect(port)
        try:
            full_metric,full_result=measure_while(c,lambda:action(control,f"BACKUP DATABASE TO 'file://{repository}'"))
        finally:control.close()
        full_job,full_seconds=full_result
        full_marker=json.loads((repository/'bases'/full_job['base']/'data'/'mydb-snapshot.json').read_text())
        full_metric.update(full_seconds=full_seconds,final_barrier_ms=full_marker['manifest']['final_barrier_ms'])
        dp,dc,_,ds=start(binary,work/f'pair-{pair}-direct.log',repository=repository,base=base)
        try:direct_metric,_=measure_while(c,lambda:[sql(dc,q) for _ in range(5) for q in queries])
        finally:stop(dp,dc)
        direct_metric['startup_seconds']=ds
        direct_metric['startup_io']=dp.startup_io
        concurrent_target=work/f'pair-{pair}-restore'
        restore_metric,_=measure_while(c,lambda:cli_run(cli,'restore','--repository',repository,'--base',base,'--target-lsn',middle['requested_lsn'],'--data-dir',concurrent_target,*key_args))
        metrics['pairs'].append({'order':order,'concurrent_reference_policy':order[-1],**measurements,'during_full':full_metric,'during_direct_queries':direct_metric,'during_restore':restore_metric})
        (work/'partial-metrics.json').write_text(json.dumps(metrics,indent=2,default=str))
    if status(c)['Effective_Policy']=='DISABLED':
        sql(c,f"ALTER MYDB BACKUP TO 'file://{repository}' SET WAL_ARCHIVE=CONTINUOUS")
        action(c,f"BACKUP DATABASE TO 'file://{repository}'")
    else:
        sql(c,f"ALTER MYDB BACKUP TO 'file://{repository}' SET WAL_ARCHIVE=CONTINUOUS")
    sql(c,'UPDATE probe SET v=v+1 WHERE id=1')
    fixed=int(status(c)['Source_LSN']);until=time.monotonic()+15
    while int(status(c)['Durable_Archived_LSN'])<fixed:
        assert time.monotonic()<until;time.sleep(.05)
    sql(c,f"ALTER MYDB BACKUP TO 'file://{repository}' SET WAL_ARCHIVE=MANUAL")
    action(c,f"ALTER MYDB BACKUP TO 'file://{repository}' SET WAL_ARCHIVE=DISABLED")
    assert not status(c)['WAL_Floor']
    bundle=work/'offline.bundle';cli_run(cli,'export','--repository',repository,'--base',base,'--target-lsn',middle['requested_lsn'],'--output',bundle,*key_args)
    stop(p,c);shutil.rmtree(work/'source')
    # Exercise OS-enforced immutability with both readers and external runtime.
    permissions={path:path.stat().st_mode & 0o777 for path in [repository,*repository.rglob('*')]}
    try:
        for path in permissions:path.chmod(0o555 if path.is_dir() else 0o444)
        ro,rconn,_,_=start(binary,work/'os-readonly.log',repository=repository,base=base)
        assert [sql(rconn,q,prepared=(i==0)) for i,q in enumerate(queries)]==model
        stop(ro,rconn)
        assert tree(data)==before
    finally:
        for path,mode in permissions.items():path.chmod(mode)
    window=work/'window.bundle'
    cli_run(cli,'export','--repository',repository,'--stream',original_stream,'--retain-pitr','7d','--output',window,*key_args)
    window_repo=work/'window-imported'
    cli_run(cli,'import','--repository',window_repo,'--input',window,*key_args)
    window_target=work/'window-restored'
    cli_run(cli,'restore','--repository',window_repo,'--stream',original_stream,'--target-lsn',middle['requested_lsn'],'--data-dir',window_target,*key_args)
    wp,wc,_,_=start(binary,work/'window-restored.log',data=window_target)
    assert [sql(wc,q) for q in queries]==expected
    stop(wp,wc)
    imported=work/'imported';cli_run(cli,'import','--repository',imported,'--input',bundle,*key_args)
    cli_run(cli,'verify','--repository',imported,'--recover-check','--base',base,'--work-dir',work/'verify-work',*key_args)
    dest=work/'offline';cli_run(cli,'restore','--repository',imported,'--base',base,'--target-lsn',middle['requested_lsn'],'--data-dir',dest,'--writable',*key_args)
    rp,rc,_,_=start(binary,work/'offline.log',data=dest);assert [sql(rc,q) for q in queries]==expected
    sql(rc,"INSERT INTO ai(v) VALUES(30)");assert sql(rc,'SELECT MAX(id) FROM ai')[0][0]>=3;stop(rp,rc)
    assert tree(data)==before
    metrics['cli_io']=CLI_IO
    metrics['checks']=['OS-enforced read-only repository','full contents and indexes','single immutable base','LSN/time PITR','E+1 sequence frame','long transaction loser undo','XA across base and source commit','concurrent capture ledger','DDL/rename/partition/GIS','temporal history at restored source time','two direct reader processes','restart retention','continuous without command','binary prepared read','GC reader lease','offline encrypted/plain bundle','retained-window export/import','writable restore AUTO_INCREMENT']
    return metrics

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--profile',default='debug');parser.add_argument('--rows',type=int,default=1000);parser.add_argument('--pairs',type=int,default=5);parser.add_argument('--encrypted',action='store_true')
    parser.add_argument('--binary',type=Path,help='Explicit server binary, including an unpacked release candidate')
    parser.add_argument('--backup-binary',type=Path,help='Explicit backup CLI from the same release candidate')
    args=parser.parse_args()
    Path('/tmp/mydb').mkdir(exist_ok=True);work=Path(tempfile.mkdtemp(prefix='backup-recovery-',dir='/tmp/mydb'))
    print(work,flush=True);binary=(args.binary or ROOT/'target'/args.profile/'mydb').resolve();cli=(args.backup_binary or ROOT/'target'/args.profile/'mydb-backup').resolve()
    try:
        metrics=scenario(binary,cli,work,args.rows,args.pairs,args.encrypted)
        metrics.update(cache_condition='fresh processes; operating system cache not flushed',profile=args.profile,pairs_requested=args.pairs)
        (work/'metrics.json').write_text(json.dumps(metrics,indent=2,default=str));print(json.dumps({'result':'PASS','artifacts':str(work),'pairs':args.pairs,'checks':metrics['checks']},ensure_ascii=False),flush=True)
    finally:
        for p in PROCESSES:
            if p.poll() is None:
                p.terminate()
                try:p.wait(timeout=10)
                except subprocess.TimeoutExpired:p.kill();p.wait()
if __name__=='__main__':main()
