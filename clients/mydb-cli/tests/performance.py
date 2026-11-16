#!/usr/bin/env python3
"""Measure client output throughput, first-row latency and bounded result memory."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cli', type=lambda p: str(Path(p).resolve()), required=True)
    parser.add_argument('--mysql', default=shutil.which('mysql'))
    parser.add_argument('--port', type=int, default=14837)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if not args.output.resolve().is_relative_to(Path('/tmp/mydb').resolve()):
        parser.error('--output must be under /tmp/mydb')
    args.output.mkdir(parents=True, exist_ok=True)
    env = {k:v for k,v in os.environ.items() if not k.startswith('MYDB_CLI_')}
    env['TMPDIR'] = str(args.output.resolve())
    endpoint = ['-h127.0.0.1', f'-P{args.port}', '-uroot', '-proot', '--ssl-mode=REQUIRED', 'test']
    clients = {'mydb-cli': [args.cli, '--no-config', *endpoint]}
    if args.mysql:
        clients['mysql'] = [args.mysql, '--no-defaults', *endpoint, '--skip-reconnect']
    records = {'platform': sys.platform, 'versions': {name: subprocess.check_output([cmd[0], '--version'], text=True).strip() for name,cmd in clients.items()}, 'samples': [], 'summary': [], 'memory': [], 'latency': []}
    def command(name, sql, flags):
        return [*clients[name], *flags, '-e', sql]
    def measured(cmd):
        wrapper = ['/usr/bin/time', '-l'] if sys.platform == 'darwin' else ['/usr/bin/time', '-v']
        start = time.perf_counter()
        result = subprocess.run([*wrapper,*cmd], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, env=env, timeout=90)
        elapsed = time.perf_counter() - start
        if result.returncode: raise RuntimeError(result.stderr.decode(errors='replace'))
        if sys.platform == 'darwin': match = re.search(rb'(\d+)\s+maximum resident set size', result.stderr); rss = int(match[1])
        else: match = re.search(rb'Maximum resident set size \(kbytes\):\s*(\d+)', result.stderr); rss = int(match[1]) * 1024
        return elapsed, rss
    cases = {
        'narrow-raw': ('SELECT n,n*2 FROM generate_series(1,100000) s(n)', ['-BNr']),
        'unicode-escaped': ("SELECT n,CONCAT('中文',CHAR(9),'x',CHAR(10),CHAR(92)) FROM generate_series(1,100000) s(n)", ['-BN']),
        'wide-raw': ("SELECT n,REPEAT('中文abc',20) FROM generate_series(1,100000) s(n)", ['-BNr']),
        'binary-raw': ("SELECT n,UNHEX(REPEAT('00FF1F',20)) FROM generate_series(1,100000) s(n)", ['-BNr']),
    }
    for case,(sql,flags) in cases.items():
        digests = {}
        for name in clients:
            result = subprocess.run(command(name,sql,flags),capture_output=True,env=env,check=True,timeout=60)
            digests[name] = hashlib.sha256(result.stdout).hexdigest()
        if len(set(digests.values())) != 1: raise AssertionError(f'output mismatch: {case}: {digests}')
        for round_no in range(5):
            order = list(clients) if round_no % 2 == 0 else list(reversed(clients))
            for name in order:
                elapsed,rss = measured(command(name,sql,flags))
                records['samples'].append({'case':case,'client':name,'round':round_no,'seconds':elapsed,'rss_bytes':rss})
        for name in clients:
            samples = [s for s in records['samples'] if s['case']==case and s['client']==name]
            records['summary'].append({'case':case,'client':name,'median_seconds':statistics.median(s['seconds'] for s in samples),'median_rows_per_second':100000 / statistics.median(s['seconds'] for s in samples),'rss_bytes':max(s['rss_bytes'] for s in samples),'sha256':digests[name]})
    for flags,mode in [(['-BNr'],'tsv'),(['-tN'],'table-spill'),(['-tNq'],'quick')]:
        for count in [100000,1000000]:
            sql = f"SELECT n,REPEAT('a',100) FROM generate_series(1,{count}) s(n)"
            elapsed,rss=measured(command('mydb-cli',sql,flags))
            records['memory'].append({'mode':mode,'rows':count,'seconds':elapsed,'rss_bytes':rss})
        values=records['memory'][-2:]
        if values[1]['rss_bytes'] > values[0]['rss_bytes'] + 16 * 1024 * 1024: raise AssertionError(f'result-sized memory growth: {values}')
    for mode,flags in [('tsv',['-BNr']),('vertical',['-EN']),('quick',['-tNq'])]:
        cmd=command('mydb-cli','SELECT * FROM generate_series(1,100000)',flags)
        start=time.perf_counter(); child=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env)
        first=child.stdout.readline(); first_time=time.perf_counter()-start
        with open(os.devnull,'wb') as sink:
            shutil.copyfileobj(child.stdout,sink)
        error=child.stderr.read(); child.wait(timeout=30)
        if not first or child.returncode: raise AssertionError(error)
        records['latency'].append({'mode':mode,'first_line_seconds':first_time})
    (args.output/'results.json').write_text(json.dumps(records,indent=2)+'\n')
    for row in records['summary']: print(row['case'],row['client'],f"{row['median_seconds']:.4f}s",f"{row['median_rows_per_second']:.0f} rows/s")
    print('Memory checks passed; detailed evidence:',args.output/'results.json')


if __name__ == '__main__':
    main()
