#!/usr/bin/env python3
"""Build and verify a local runtime image from an already validated Linux build.

Uses the repository Dockerfile's runtime stage verbatim except artifact COPY
sources. This avoids recompiling an immutable ELF during distribution testing.
Only owned containers, data directories and loopback ports are used.
"""
import argparse
from decimal import Decimal
import hashlib
import json
import re
import shlex
from pathlib import Path
import shutil
import subprocess
import time
import uuid

import pymysql
from test_native_readiness import free_port, query
from build_linux_glibc217 import source_hash

ROOT = Path(__file__).resolve().parent.parent


def sha(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def command(args, timeout=120):
    result = subprocess.run(list(map(str,args)),capture_output=True,text=True,timeout=timeout)
    if result.returncode:
        raise RuntimeError(f'{args}: {result.returncode}\n{result.stdout}\n{result.stderr}')
    return result.stdout.strip()


def local_runtime_build(context, image, base, names, log):
    """Execute the runtime instructions using a cached immutable base.

    Docker Desktop's BuildKit resolver can require an unavailable registry
    even when docker run already has the pinned base locally.
    """
    builder = 'mydb-image-builder-' + uuid.uuid4().hex[:10]
    names.append(builder)
    command(['docker','run','-d','--name',builder,'--platform','linux/amd64',
             '--user','0:0','--entrypoint','sleep',base,'infinity'])
    instructions = []
    pending = ''
    for line in (context/'Dockerfile').read_text().splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        pending += line.removesuffix('\\') + ' '
        if not line.endswith('\\'):
            instructions.append(pending.strip())
            pending = ''
    assert not pending
    changes = []
    for instruction in instructions:
        op, value = instruction.split(None, 1)
        log.write(instruction+'\n'); log.flush()
        if op in ('ARG','FROM'):
            continue
        if op == 'RUN':
            log.write(command(['docker','exec',builder,'sh','-ec',value],timeout=600)+'\n')
        elif op == 'COPY':
            fields=shlex.split(value); mode=None
            if fields[0].startswith('--chmod='):
                mode=fields.pop(0).split('=',1)[1]
            assert len(fields)==2, 'unsupported runtime COPY'
            source=(context/fields[0]).resolve()
            assert source.is_relative_to(context.resolve())
            command(['docker','cp',source,builder+':'+fields[1]],timeout=180)
            if mode:command(['docker','exec',builder,'chmod',mode,fields[1]])
        elif op == 'STOPSIGNAL':
            assert value == 'SIGTERM', 'local builder only supports Docker default SIGTERM'
        elif op in ('LABEL','USER','WORKDIR','VOLUME','EXPOSE','ENTRYPOINT','CMD','ENV'):
            changes += ['--change',instruction]
        else:
            raise ValueError('unsupported runtime instruction: '+op)
    log.write(command(['docker','commit',*changes,builder,image],timeout=180)+'\n')
    command(['docker','stop',builder]); command(['docker','rm',builder]); names.remove(builder)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--base-image', required=True, help='immutable Debian runtime reference')
    parser.add_argument('--local-runtime-build', action='store_true',
                        help='execute the same runtime instructions using a cached immutable base without BuildKit resolution')
    parser.add_argument('--native-volume', action='store_true',
                        help='use an owned Linux volume and export its contents before cleanup')
    args = parser.parse_args()
    if not re.fullmatch(r'(?:[^\s@]+@)?sha256:[0-9a-f]{64}', args.base_image):
        parser.error('--base-image must be immutable')
    build, root = args.build.resolve(), args.output.resolve()
    if not root.is_relative_to(Path('/tmp/mydb').resolve()):
        parser.error('--output must be below /tmp/mydb')
    root.mkdir(parents=True,exist_ok=False)
    result = {'passed':False,'base_image':args.base_image,'script_sha256':sha(Path(__file__).resolve())}
    names = []
    containers = 'mydb-release-' + uuid.uuid4().hex[:10]
    image = 'mydb-candidate:1.0.0-' + containers[-10:]
    clients = []
    volume = containers + '-workspace' if args.native_volume else None
    volume_created = False
    result['storage_mode'] = 'volume' if volume else 'bind'

    def connect(port, password='image-password'):
        conn = pymysql.connect(host='127.0.0.1',port=port,user='release_image',password=password,database='test',
                               autocommit=True,read_timeout=60,write_timeout=60,
                               ssl={'check_hostname':False},connect_timeout=3)
        clients.append(conn)
        return conn

    def ready(port, name):
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if command(['docker','inspect','-f','{{.State.Running}}',name]) != 'true':
                raise RuntimeError(command(['docker','logs',name]))
            try:
                return connect(port)
            except pymysql.Error:
                time.sleep(.1)
        raise TimeoutError('container SQL startup')

    def start(suffix, data, extra=None):
        name, port = containers + '-' + suffix, free_port()
        names.append(name)
        mounts = ['-v',f'{data}:/var/lib/mydb','-v',f'{root / "backups"}:/backups',
                  '-v',f'{root / "restore"}:/restore']
        if volume:
            mounts = []
            for source, destination in [(data.name,'/var/lib/mydb'),('backups','/backups'),('restore','/restore')]:
                mounts += ['--mount',f'type=volume,src={volume},dst={destination},volume-subpath={source}']
        argv=['docker','run','-d','--name',name,'--platform','linux/amd64','--cpus','2','--memory','1g',
              '-p',f'127.0.0.1:{port}:8738',*mounts,
              '-e','MYDB_ROOT_PASSWORD=container-root','-e','MYDB_DURABILITY=strict',
              '-e','MYDB_MEMORY_TARGET=256M','-e','MYDB_TEMP_DISK_RESERVE_PCT=1',image]
        command(argv+(extra or []))
        cli=['docker','exec','-i','-e','MYSQL_PWD=container-root',name,'mysql',
             '-h127.0.0.1','-P8738','-uroot','--ssl','--skip-ssl-verify-server-cert','--batch','--skip-column-names']
        deadline=time.monotonic()+60
        while time.monotonic()<deadline:
            initialized=subprocess.run(cli,input="SELECT COUNT(*) FROM mysql.user WHERE User='release_image' AND Host='%';",
                                       capture_output=True,text=True,timeout=15)
            if initialized.returncode==0:break
            time.sleep(.2)
        else:raise RuntimeError(initialized.stderr)
        if initialized.stdout.strip()=='0':
            created=subprocess.run(cli,input="CREATE USER 'release_image'@'%' IDENTIFIED BY 'image-password'; GRANT ALL PRIVILEGES ON *.* TO 'release_image'@'%' WITH GRANT OPTION; GRANT BACKUP_ADMIN,SYSTEM_VARIABLES_ADMIN ON *.* TO 'release_image'@'%';",
                                   capture_output=True,text=True,timeout=30)
            assert created.returncode==0,created.stderr
        else:assert initialized.stdout.strip()=='1'
        return name, port, ready(port,name)

    expected=((1,Decimal('1.2500'),b'\x00\xff'),(2,Decimal('-2.5000'),b'p'*20000),(3,None,None))
    def verify(conn, rows=expected):
        assert query(conn,'SELECT * FROM image_rows ORDER BY id') == rows
        assert query(conn,'SELECT id FROM image_rows FORCE INDEX(kv) WHERE v>0 ORDER BY id') == tuple((row[0],) for row in rows if row[1] is not None and row[1]>0)

    try:
        manifest=json.loads((build/'build-manifest.json').read_text())
        assert source_hash(ROOT)==manifest['source_sha256']
        assert 'third-party/packages.json' in manifest['artifacts'], 'license payload is not bound to build manifest'
        for name,digest in manifest['artifacts'].items():
            assert sha(build/'bin'/name)==digest,name
        result['source_sha256']=manifest['source_sha256']
        result['linux_artifacts']=manifest['artifacts']
        context=root/'context';context.mkdir()
        for name in ('mydb','mydb-backup','mydb-cli'):shutil.copy2(build/'bin'/name,context/name)
        shutil.copytree(build/'bin/third-party',context/'third-party')
        shutil.copytree(ROOT/'docker',context/'docker')
        shutil.copy2(ROOT/'LICENSE',context/'LICENSE')
        original=(ROOT/'Dockerfile').read_text()
        result['source_dockerfile_sha256']=sha(ROOT/'Dockerfile')
        runtime='ARG RUNTIME_IMAGE\nFROM ${RUNTIME_IMAGE} AS runtime'+original.split('FROM ${RUNTIME_IMAGE} AS runtime',1)[1]
        runtime=runtime.replace('COPY --from=builder /out/','COPY ')
        assert '--from=builder' not in runtime
        (context/'Dockerfile').write_text(runtime)
        result['runtime_dockerfile_sha256']=sha(context/'Dockerfile')
        if args.local_runtime_build:
            with (root/'build-local.log').open('w') as output:
                local_runtime_build(context,image,args.base_image,names,output)
            result['image_build_method']='runtime RUN/COPY and image configuration from the generated Dockerfile; cached immutable base'
        else:
            with (root/'build.log').open('wb') as output:
                subprocess.run(['docker','build','--platform','linux/amd64','--build-arg',f'RUNTIME_IMAGE={args.base_image}',
                                '-t',image,str(context)],stdout=output,stderr=subprocess.STDOUT,check=True,timeout=600)
        config=json.loads(command(['docker','image','inspect',image]))[0]
        assert config['Architecture']=='amd64'
        assert config['Config']['User']=='10001:10001'
        assert '/var/lib/mydb' in config['Config']['Volumes']
        result['image_id']=config['Id']
        for name in ('data','backups','restore'):(root/name).mkdir()
        if volume:
            command(['docker','volume','create',volume]);volume_created=True
        command(['docker','run','--rm','--user','0:0','--entrypoint','sh',
                 '-v',f'{volume or root}:/owned',image,'-ec',
                 'mkdir -p /owned/data /owned/backups /owned/restore; chown 10001:10001 /owned/data /owned/backups /owned/restore'])
        name,port,conn=start('source',root/'data')
        assert command(['docker','exec',name,'id','-u'])=='10001'
        assert conn._sock.cipher()
        command(['docker','exec',name,'sh','-ec','test -s /usr/share/licenses/mydb/third-party/packages.json; test -s /usr/local/bin/mydb-backup; test -s /usr/local/bin/mydb-cli'])
        assert command(['docker','exec',name,'mydb-cli','--version']) == 'mydb-cli 1.0.0'
        query(conn,'CREATE TABLE image_rows(id INT PRIMARY KEY,v DECIMAL(20,4),b LONGBLOB,KEY kv(v))')
        with conn.cursor() as cursor:cursor.executemany('INSERT INTO image_rows VALUES(%s,%s,%s)',expected)
        verify(conn)
        try:
            unexpected=connect(port,'wrong-password');unexpected.close()
        except pymysql.MySQLError as error:assert error.args[0]==1045
        else:raise AssertionError('wrong password accepted')
        query(conn,'ALTER SYSTEM CHECKPOINT')
        command(['docker','stop','-t','60',name],timeout=75)
        assert command(['docker','inspect','-f','{{.State.ExitCode}}',name])=='0'
        command(['docker','start',name]);conn=ready(port,name);verify(conn)
        writer=connect(port);query(writer,'BEGIN');query(writer,'UPDATE image_rows SET v=9000')
        query(conn,'ALTER SYSTEM CHECKPOINT')
        command(['docker','kill','--signal','KILL',name])
        assert command(['docker','inspect','-f','{{.State.ExitCode}}',name])=='137'
        command(['docker','start',name]);conn=ready(port,name);verify(conn)
        request=query(conn,"BACKUP DATABASE TO 'file:///backups/repository'")[0][0]
        deadline=time.monotonic()+180
        while time.monotonic()<deadline:
            job=next(row for row in query(conn,'SHOW MYDB BACKUP JOBS') if row[0]==request)
            if job[2]=='COMPLETED':break
            assert job[2]=='RUNNING',job
            time.sleep(.1)
        else:raise TimeoutError('container backup did not complete')
        base=job[4];assert base
        cli=['docker','exec',name,'mydb-backup']
        command(cli+['verify','--repository','/backups/repository','--base',base],timeout=180)
        command(cli+['restore','--repository','/backups/repository','--base',base,'--data-dir','/restore/data'],timeout=180)
        command(cli+['promote','--data-dir','/restore/data'],timeout=180)
        clone,clone_port,clone_conn=start('restored',root/'restore',['--data-dir','/var/lib/mydb/data'])
        verify(clone_conn)
        query(clone_conn,"INSERT INTO image_rows VALUES(4,4.1250,X'00FF')")
        updated=expected+((4,Decimal('4.1250'),b'\x00\xff'),)
        verify(clone_conn,updated)
        query(clone_conn,'ALTER SYSTEM CHECKPOINT');command(['docker','stop','-t','60',clone],timeout=75)
        command(['docker','start',clone]);clone_conn=ready(clone_port,clone);verify(clone_conn,updated)
        result.update(passed=True,checks=['amd64 architecture','UID/GID 10001','persistent volume','config and password initialization',
            'TLS and typed SQL/indexes','SIGTERM restart','SIGKILL loser recovery','license payload','backup CLI verify/restore/promote',
            'restored writes and restart'])
        command(['docker','save','-o',root/'image.tar',image],timeout=300)
        result['image_archive_sha256']=sha(root/'image.tar')
    except BaseException as error:
        result['error']=repr(error)
        raise
    finally:
        for conn in clients:
            try:conn.close()
            except pymysql.Error:pass
        for name in names:
            subprocess.run(['docker','stop','-t','15',name],capture_output=True,timeout=30)
        for name in names:
            logs=subprocess.run(['docker','logs',name],capture_output=True,text=True)
            (root/(name+'.log')).write_text(logs.stdout+logs.stderr)
            subprocess.run(['docker','rm',name],capture_output=True)
        if volume_created:
            exporter=containers+'-export'
            try:
                command(['docker','create','--name',exporter,'--entrypoint','sh',
                         '-v',volume+':/evidence:ro',image])
                command(['docker','cp',exporter+':/evidence/.',root/'native-workspace'],timeout=180)
                command(['docker','rm','-v',exporter]);command(['docker','volume','rm',volume])
            except BaseException as error:
                result['passed']=False;result['retained_volume']=volume
                result['cleanup_error']=repr(error)
                subprocess.run(['docker','rm','-v',exporter],capture_output=True,timeout=30)
        (root/'results.json').write_text(json.dumps(result,indent=2,default=str)+'\n')


if __name__=='__main__':main()
