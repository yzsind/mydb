#!/usr/bin/env python3
"""Syscall fault gates for WAL commit and heap checkpoint persistence.

The interposer is injected with DYLD_INSERT_LIBRARIES on macOS and with
LD_PRELOAD on Linux; the gated operations, marker protocol and recovery
assertions are identical on both platforms.
"""
import argparse
import hashlib
import platform
from pathlib import Path
import subprocess

from run_fault_matrix import ROOT, Servers, query, free_port, kill
from safety_evidence import write_json, source_identity, snapshot
import pymysql

DARWIN = platform.system() == 'Darwin'
LINUX = platform.system() == 'Linux'


def scenario(binary, inject, library, root, target, mode, buffering):
    root.mkdir()
    servers = Servers(binary, root)
    marker = root/'armed-fault'
    env = {'MYDB_WAL_TXN_BUFFER': str(buffering), 'MYDB_CHECKPOINT_INTERVAL_MS':'3600000',
           'MYDB_UNDO_PURGE_INTERVAL_MS':'3600000', 'MYDB_UNDO_RETENTION_MS':'3600000'}
    fault_env = {**env, **inject,
                 'MYDB_STORAGE_GATE_ROOT': str(root/'node'/target), 'MYDB_STORAGE_GATE_MARKER': str(marker)}
    result = {'status':'incomplete','target':target,'mode':mode,'buffering':buffering}
    try:
        port=free_port()
        process, conn=servers.start('node',port,fault_env)
        query(conn,'CREATE TABLE t(id INT PRIMARY KEY,v INT,KEY by_v(v))')
        query(conn,'INSERT INTO t VALUES(1,10),(2,20)')
        query(conn,'ALTER SYSTEM CHECKPOINT')
        query(conn,'BEGIN')
        query(conn,'UPDATE t SET v=v+100')
        if target != 'system/wal':
            query(conn,'COMMIT')
        marker.write_text(mode)
        failed=False
        try:
            query(conn,'COMMIT' if target=='system/wal' else 'ALTER SYSTEM CHECKPOINT')
        except pymysql.MySQLError as error:
            failed=True
            result['sql_error']=list(error.args)
            assert error.args[0] in (1030,3675,2006,2013), error.args
        assert not marker.exists(), 'configured fault did not fire'
        assert failed == (mode != 'write-short'), ('swallowed I/O error or short-write retry failed', result)
        kill(process)
        process, conn=servers.start('node',port,env)
        actual=query(conn,'SELECT id,v FROM t ORDER BY id')
        before=((1,10),(2,20));after=((1,110),(2,120))
        assert actual in (before,after), ('partial commit',actual)
        if target!='system/wal' or not failed:
            assert actual==after, ('acknowledged commit lost',actual)
        assert query(conn,'SELECT id,v FROM t FORCE INDEX(by_v) ORDER BY v')==actual
        query(conn,'ALTER SYSTEM CHECKPOINT')
        kill(process)
        process, conn=servers.start('node',port,env)
        assert query(conn,'SELECT id,v FROM t ORDER BY id')==actual
        query(conn,'INSERT INTO t VALUES(3,30)')
        evidence=(root/'node.log').read_text(errors='replace')
        assert f'[storage-gate] injected {mode} into ' in evidence
        result.update(status='pass', fault_consumed=True, repeated_recovery=True, final_rows=actual)
    except BaseException as error:
        result.update(status='fail',error=repr(error))
        raise
    finally:
        write_json(root/'results.json',result)
        for process in servers.processes:
            if process.poll() is None: kill(process)
        servers.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    args.binary,args.output=args.binary.resolve(),args.output.resolve()
    if DARWIN:
        library, build = args.output/'storage-fault.dylib', ['clang','-Wall','-Wextra','-Werror','-dynamiclib']
    elif LINUX:
        library, build = args.output/'storage-fault.so', ['cc','-Wall','-Wextra','-Werror','-shared','-fPIC','-ldl']
    else:
        parser.error('the storage fault interposer requires macOS or Linux')
    if not args.output.is_relative_to(Path('/tmp/mydb').resolve()): parser.error('output must be below /tmp/mydb')
    args.output.mkdir(parents=True,exist_ok=False)
    injection = {'DYLD_INSERT_LIBRARIES' if DARWIN else 'LD_PRELOAD': str(library)}
    paths=[ROOT/'scripts/fixtures/storage_io_fault.c',Path(__file__),ROOT/'scripts/test_native_readiness.py']
    snapshot(ROOT,args.output,paths)
    subprocess.run([*build,str(paths[0]),'-o',str(library)],check=True)
    write_json(args.output/'manifest.json',dict(source_identity(ROOT,paths),binary_sha256=hashlib.sha256(args.binary.read_bytes()).hexdigest(),library_sha256=hashlib.sha256(library.read_bytes()).hexdigest()))
    plan=[(target,mode,b) for target in ('system/wal','objects/tables') for mode in ('fsync-eio','fsync-enospc','write-enospc','write-short') for b in (0,1)]
    results=[{'target':t,'mode':m,'buffering':b,'status':'incomplete'} for t,m,b in plan]
    write_json(args.output/'results.json',results)
    for index,(target,mode,b) in enumerate(plan):
        try:
            scenario(args.binary,injection,library,args.output/f'{target.replace("/","-")}-{mode}-{b}',target,mode,b)
            results[index]['status']='pass'
        except Exception as error: results[index].update(status='fail',error=repr(error))
        write_json(args.output/'results.json',results)
        print(results[index],flush=True)
    return 0 if all(r['status']=='pass' for r in results) else 1


if __name__=='__main__': raise SystemExit(main())
