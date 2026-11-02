#!/usr/bin/env python3
"""Reproducible XML-function differential checks and local performance comparison.

Requires PyMySQL; use a virtualenv under /tmp/mydb. Both servers must be local.
All generated data and measurements are written below --directory.
"""
from __future__ import annotations
import argparse
import concurrent.futures
import hashlib
import json
import math
import os
from pathlib import Path
import random
import statistics
import subprocess
import time
import pymysql

ROOT = Path(__file__).resolve().parents[1]
SESSION = ["SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci",
           "SET SESSION sql_mode='NO_ENGINE_SUBSTITUTION,ERROR_FOR_DIVISION_BY_ZERO'"]


def quote(text):
    return "'" + text.replace("\\", "\\\\").replace("'", "''") + "'"


def cases():
    documents = ["", "plain text", "<a/>", "<a>x<b>y</b>z</a>",
        '<a><b c="1">11</b><b c="2">22</b><c><b>33</b></c></a>',
        '<a c="1" c="2"><b>1</b><b>2</b><c>3</c></a>',
        '<a> &amp;&#65; &unknown; <![CDATA[x<y]]><!-- c --> tail </a>',
        '<a><b/></a><a><b>second</b></a>', '<a xmlns:x="urn:x"><x:b>中文é</x:b></a>',
        '<?xml version="1.0"?><a/>', '<!DOCTYPE a SYSTEM "x"><a/>',
        '<a>x</z>', '<a>x</long>', '<a><b', '<a b="x>', '<a><b/></a><trailing',
        '<a a=unquoted/>', '<a a="" b=" x "/>', '<a/ >', '<a>x</a >', '<a /  >', '<a>\n<b>x</long></a>', '<a a="x"/>tail']
    paths = ["/", "/a", "a/b", "//b", "//*", "/a/*", "/a/@c", "//@*", "/a/text()",
        "/a/node()", "/a/comment()", "/a/child::text()", "/a/child::*", "/a/self::*",
        "/a/descendant::*", "/a/descendant-or-self::*", "/a/b/parent::*",
        "/a/b/ancestor::*", "/a/b/ancestor-or-self::*", "/a/following::b",
        "/a/namespace::*", "/a/b/..", "/a/b/../c", "//b|//c", "//b|//b",
        "//b[1]", "//b[2]", "//b[last()]", "//b[position()=2]", "//b[1][1]",
        "//b[1.9]", "//b[2 div 2]", "//b[@c]", "//b[@c='2']", "//b[@c!='2']",
        "//b[@c>1]", "//b[2>@c]", "//b[@c or ../@c]", "//b[not(@c)]",
        "//b[boolean(@c)]", "//b[contains(.,'2')]", "//b[count(../b)=2]",
        "//b[../@c='1']", "/a[b]", "count(//b)", "count(/)", "sum(//b)",
        "boolean(//b)", "number(//b)", "number()", "true()", "not(false())",
        "contains('abc','b')", "concat('a','b','ignored')", "substring('abcd',-2)",
        "string-length('中文')", "round(1.5)", "ceiling(1.2)", "floor(-1.2)",
        "1+2*3", "5 div 2", "5 mod 2", "-1", "1 div 0", "'literal'", "3.25",
        ".", "/a/./b", "/a/../a", "//b/ancestor::*[1]", "//b/ancestor-or-self::*[2]",
        "//b/parent::a", "//b/self::*[1]", "//b[count(.)]", "//b[count(.)=last()]",
        "//b[count(.)-1]", "//b[position()=count(.)]", "//b[true()][2]",
        "contains($@missing,'')", "number($@missing)", "boolean('word')",
        "'x'='x'", "0.000+1", "1.00*2.0", "1.234567890123456789", "1.0000000000000000000000000000001",
        "-0.000", "1.200 mod 0.11", "concat(1.00,2.000)", "1.200 mod 0",
        "9223372036854775808", "18446744073709551616", "9"*350, "9"*350+".1"]
    for di, document in enumerate(documents):
        for pi, path in enumerate(paths):
            for fn in ["EXTRACTVALUE", "UPDATEXML"]:
                arguments = f"{quote(document)},{quote(path)}"
                if fn == "UPDATEXML": arguments += ",'<r>replacement</r>'"
                yield {"id": f"{fn.lower()}-{di}-{pi}", "sql": f"SELECT {fn}({arguments}) AS v"}
    for ni,name in enumerate(['中文','é','😀','α','a😀','á','１２','a١','©','·x','𝒜','́']):
        yield {"id":f"unicode-name-{ni}","sql":f"SELECT EXTRACTVALUE({quote('<'+name+'>v</'+name+'>')},{quote('//'+name)}) AS v"}
    for pi,path in enumerate(['count (/a)','/a/child ::b','/a/child: :b','/a/text ()','//and','//or','//div','/a/count(/a)','/a/.[1]','/a/..[1]','/a/noaxis::b','1 and(true())','/a[1 . 0]']):
        yield {"id":f"lexical-{pi}","sql":f"SELECT EXTRACTVALUE('<a><b>x</b></a>',{quote(path)}) AS v"}
    for i, path in enumerate(["", "!", "/&a", "//", "/a[", "/a/", "foo()", "id('x')",
        "starts-with('x','x')", "last()", "position()", "string-length()", "count(1)",
        "//b[@c=@d]", "1/a", "$missing", "/a[.123]", "(//a)[1]", "//a!", "//a\t"]):
        for fn in ["EXTRACTVALUE", "UPDATEXML"]:
            yield {"id": f"invalid-{fn}-{i}", "sql": f"SELECT {fn}('<a><b c=\"1\"/></a>',{quote(path)}" + (",'x'" if fn == "UPDATEXML" else "") + ") AS v"}
    for fn, count in [("EXTRACTVALUE",2),("UPDATEXML",3)]:
        for n in range(4):
            if n != count: yield {"id":f"arity-{fn}-{n}","sql":f"SELECT {fn}({','.join([quote('x')]*n)}) AS v"}
        for pos in range(count):
            arguments=["'<a>x</a>'","'/a'","'new'"][:count];arguments[pos]="NULL"
            yield {"id":f"null-{fn}-{pos}","sql":f"SELECT {fn}({','.join(arguments)}) AS v"}
    extras = [
        "SELECT EXTRACTVALUE(_binary'<a>é</a>','/a') AS v",
        "SELECT EXTRACTVALUE(_latin1'<a>é</a>',_latin1'/a') AS v",
        "SELECT EXTRACTVALUE('<a>AbC</a>','contains(/a,\"abc\")') AS v",
        "SELECT EXTRACTVALUE('<a>AbC</a>' COLLATE utf8mb4_bin,'contains(/a,\"abc\")') AS v",
        "SELECT EXTRACTVALUE(CONVERT('<a/>' USING ucs2),'/a') AS v",
        "SELECT EXTRACTVALUE(NULL,'!') AS v",
        "SELECT UPDATEXML(NULL,'!','x') AS v",
        "SELECT EXTRACTVALUE(NULL,NULL) AS v",
        "SELECT EXTRACTVALUE(1,1) AS v",
        "SELECT EXTRACTVALUE(NULL,1) AS v",
        "SELECT REPLACE(EXTRACTVALUE('1','/a'),'ds','') AS v",
        "SELECT AVG(DISTINCT EXTRACTVALUE('','1')) AS v",
        "SELECT EXTRACTVALUE('',LPAD(0.1111E-15,2011,1)) AS v",
        "SELECT UPDATEXML(CONVERT(_latin1'<' USING utf8),'1','1') AS v",
        "SELECT EXTRACTVALUE('<a/>','/a[@c=$@missing]') AS v",
    ]
    for i, sql in enumerate(extras): yield {"id":f"extra-{i}","sql":sql}
    for i in [1,2,3]:
        yield {"id":f"user-variable-{i}","setup":[f"SET @i={i}"],"sql":"SELECT EXTRACTVALUE('<a><b>X</b><b>Y</b></a>','//b[$@i]') AS v"}
    yield {"id":"path-user-variable", "setup":["SET @p='/a'"],"sql":"SELECT EXTRACTVALUE('<a>x</a>',@p) AS v"}
    for depth in [253,254,255]:
        doc='<a>'*depth+'x'+'</a>'*depth
        yield {"id":f"depth-{depth}","sql":f"SELECT EXTRACTVALUE({quote(doc)},'//a[1]') AS v"}


def connection(port, password):
    conn = pymysql.connect(host="127.0.0.1",port=port,user="root",password=password,
                           charset="utf8mb4",use_unicode=False,autocommit=True,read_timeout=600)
    with conn.cursor() as cursor:
        for sql in SESSION: cursor.execute(sql)
    return conn


def cell(v):
    if isinstance(v,bytes): return {"hex":v.hex()}
    if v is None: return None
    return str(v)


def outcome(conn, case):
    try:
        with conn.cursor() as cursor:
            for sql in case.get("setup",[]): cursor.execute(sql)
            cursor.execute(case["sql"])
            rows=[[cell(v) for v in row] for row in cursor.fetchall()]
            fields=[{k:getattr(f,k) for k in ["name","type_code","length","charsetnr","flags","scale"]} for f in getattr(cursor._result,"fields",None) or []]
            result={"status":"ok","rows":rows,"columns":fields}
    except pymysql.MySQLError as error:
        result={"status":"error","errno":error.args[0],"message":error.args[1],"sqlstate":getattr(error,"sqlstate",None)}
    with conn.cursor() as cursor:
        cursor.execute("SHOW WARNINGS")
        result["warnings"]=[[v.decode('utf8','replace') if isinstance(v,bytes) else v for v in row] for row in cursor.fetchall()]
    return result


def differential(args):
    oracle=connection(args.mysql_port,args.mysql_password)
    target=connection(args.mydb_port,args.mydb_password) if not args.capture_only else None
    entries=[];failures=[]
    for case in cases():
        expected=outcome(oracle,case)
        entries.append(dict(case,expected=expected))
        if target:
            actual=outcome(target,case)
            if actual!=expected: failures.append(dict(case,expected=expected,actual=actual))
    args.directory.mkdir(parents=True,exist_ok=True)
    (args.directory/'oracle.json').write_text(json.dumps({"mysql_version":"8.0.46","session":SESSION,"cases":entries},ensure_ascii=False,indent=2)+'\n')
    (args.directory/'differences.json').write_text(json.dumps(failures,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({"cases":len(entries),"differences":len(failures),"directory":str(args.directory)}))
    for f in failures[:10]: print(json.dumps(f,ensure_ascii=False))
    return not failures and (prepared_differential(args) if target else True)


class Worker:
    def __init__(self,args,port,password,database='xml_function_bench'):
        env=dict(os.environ,FEB_PASSWORD=password)
        self.process=subprocess.Popen([str(args.worker),str(port)]+([database] if database else []),env=env,
            stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True)
        assert json.loads(self.process.stdout.readline())['event']=='connected'
        for sql in SESSION: self.query(sql)
    def request(self,request):
        self.process.stdin.write(json.dumps(request)+'\n');self.process.stdin.flush()
        assert json.loads(self.process.stdout.readline())['event']=='started'
        return json.loads(self.process.stdout.readline())
    def query(self,sql,capture=None):
        result=self.request(dict(sql=sql,capture=str(capture) if capture else None,sample_limit=0))
        if result['status']!='ok': raise RuntimeError(result)
        return result['elapsed_ms']
    def close(self):
        self.process.stdin.close();self.process.wait(timeout=10)


def prepared_differential(args):
    workers=[Worker(args,args.mysql_port,args.mysql_password,None),Worker(args,args.mydb_port,args.mydb_password,None)]
    requests=[]
    for params in [['<a>x</a>','/a'],[None,None],[1,1],[None,None],['<b>next</b>','/b'],['<a/>','!'],['<a/>','count(//a)']]:
        requests.append(dict(sql='SELECT EXTRACTVALUE(?,?) AS v',params=params,prepared=True,capture_prepared_metadata=True))
    for params in [['<a>x</a>','/a','<b/>'],[None,None,None],[1,1,1],[None,None,None],['<a>x</a>','/a',None],['<a/>','!','x']]:
        requests.append(dict(sql='SELECT UPDATEXML(?,?,?) AS v',params=params,prepared=True,capture_prepared_metadata=True))
    for sql in ["SELECT EXTRACTVALUE('<a/>','!') AS v","SELECT EXTRACTVALUE('<a/>','//a[@x=@y]') AS v"]:
        requests.append(dict(sql=sql,prepared=True,capture_prepared_metadata=True))
    failures=[]
    for index,request in enumerate(requests):
        results=[worker.request(dict(request,sample_limit=4)) for worker in workers]
        for result in results: result.pop('elapsed_ms',None)
        if results[0]!=results[1]: failures.append(dict(id=f'prepared-{index}',request=request,expected=results[0],actual=results[1]))
        diagnostics=[worker.request(dict(sql='SHOW WARNINGS',sample_limit=16)) for worker in workers]
        diagnostics=[{k:r.get(k) for k in ['status','rows','sample']} for r in diagnostics]
        if diagnostics[0]!=diagnostics[1]: failures.append(dict(id=f'prepared-warnings-{index}',expected=diagnostics[0],actual=diagnostics[1]))
    for worker in workers:worker.close()
    (args.directory/'prepared-differences.json').write_text(json.dumps(failures,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(dict(prepared_cases=len(requests),prepared_differences=len(failures))))
    for failure in failures[:8]: print(json.dumps(failure,ensure_ascii=False))
    return not failures


def benchmark(args):
    args.directory.mkdir(parents=True,exist_ok=True)
    connections=[connection(args.mysql_port,args.mysql_password),connection(args.mydb_port,args.mydb_password)]
    for conn in connections:
        with conn.cursor() as q:
            q.execute('CREATE DATABASE IF NOT EXISTS xml_function_bench');q.execute('USE xml_function_bench')
    specs=[]
    for size in [256,4096,65536]:
        rows=min(100000,max(1024,args.data_bytes//size))
        table=f'xml_{size}'
        records=[]
        for i in range(rows):
            prefix=f'<a id="{i}"><b c="1">{i}</b><b c="2">{i+1}</b><c><b>{i+2}</b></c><pad>'
            document=prefix+('p'*max(0,size-len(prefix)-len('</pad></a>')) )+'</pad></a>'
            records.append((i,document))
        for conn in connections:
            with conn.cursor() as q:
                q.execute(f'DROP TABLE IF EXISTS {table}')
                q.execute(f'CREATE TABLE {table}(id BIGINT PRIMARY KEY,doc LONGTEXT)')
                for start in range(0,rows,128): q.executemany(f'INSERT INTO {table} VALUES(%s,%s)',records[start:start+128])
        paths={'child':'/a/b','descendant':'//b','attribute':'/a/@id','position':'/a/b[2]',
               'union':'/a/b|/a/c/b','predicate':"/a/b[@c='2' and contains(.,'1')]"}
        for shape,path in paths.items():
            specs.append((f'extract-{size}-{shape}',f"SELECT id,EXTRACTVALUE(doc,{quote(path)}) FROM {table}",rows))
            for matches,update_path in [('zero',path+'missing'),('one',path+'[1]'),('many','/a/b')]:
                # Cardinality cases stay valid XPath for every shape.
                if matches=='zero': update_path='/a/missing'
                if matches=='one' and shape=='union': update_path='/a/c/b|/a/missing'
                if matches=='one' and shape=='position': update_path='/a/b[2]'
                if matches=='one' and shape=='descendant': update_path='/descendant::b[1]'
                if matches=='one' and shape=='child': update_path='/a/b[1]'
                if matches=='many' and shape in ['descendant','union']: update_path=path
                specs.append((f'update-{size}-{shape}-{matches}',f"SELECT id,UPDATEXML(doc,{quote(update_path)},'<r/>') FROM {table}",rows))
        del records
    workers=[Worker(args,args.mysql_port,args.mysql_password),Worker(args,args.mydb_port,args.mydb_password)]
    measurements=[]
    for label,sql,rows in specs:
        captures=[args.directory/(label+'-'+engine+'.jsonl') for engine in ['mysql','mydb']]
        for worker,path in zip(workers,captures): worker.query(sql,path)
        hashes=[hashlib.sha256(path.read_bytes()).hexdigest() for path in captures]
        if hashes[0]!=hashes[1]: raise AssertionError(f'correctness mismatch: {label}')
        for _ in range(3):
            for worker in workers: worker.query(sql)
        times=[[],[]]
        for repeat in range(9):
            for engine in ([0,1] if repeat%2==0 else [1,0]): times[engine].append(workers[engine].query(sql))
        medians=[statistics.median(t) for t in times]
        result=dict(case=label,rows=rows,mysql_ms=medians[0],mydb_ms=medians[1],speedup=medians[0]/medians[1],samples=times,sql=sql,sha256=hashes[0])
        measurements.append(result);print(json.dumps(result),flush=True)
        for path in captures: path.unlink()
        (args.directory/'benchmark.json').write_text(json.dumps(measurements,indent=2)+'\n')
    rng=random.Random(20261102)
    bootstrap=[]
    for _ in range(10000):
        ratios=[statistics.median(rng.choices(m['samples'][0],k=9))/statistics.median(rng.choices(m['samples'][1],k=9)) for m in measurements]
        bootstrap.append(math.exp(statistics.mean(math.log(r) for r in ratios)))
    geo=math.exp(statistics.mean(math.log(m['speedup']) for m in measurements))
    lower=sorted(bootstrap)[250]
    passed=lower>1 and all(m['speedup']>=1/1.05 for m in measurements)
    summary=dict(geometric_speedup=geo,confidence_95_lower=lower,passed=passed,cases=len(measurements))
    (args.directory/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary))
    # Separate concurrent-client throughput, using the same serial query.
    sql=specs[0][1]
    concurrent_results={}
    for label,port,password in [('mysql',args.mysql_port,args.mysql_password),('mydb',args.mydb_port,args.mydb_password)]:
        group=[Worker(args,port,password) for _ in range(4)]
        start=time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda worker:[worker.query(sql) for _ in range(9)],group))
        concurrent_results[label]=dict(queries=36,elapsed_seconds=time.monotonic()-start)
        for worker in group: worker.close()
    (args.directory/'concurrent.json').write_text(json.dumps(concurrent_results,indent=2)+'\n')
    for worker in workers: worker.close()
    return passed


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode',choices=['diff','bench'])
    parser.add_argument('--mysql-port',type=int,default=33926)
    parser.add_argument('--mydb-port',type=int,default=34926)
    parser.add_argument('--mysql-password',default=os.environ.get('XML_MYSQL_PASSWORD',''))
    parser.add_argument('--mydb-password',default=os.environ.get('XML_MYDB_PASSWORD','root'))
    parser.add_argument('--directory',type=Path,default=Path('/tmp/mydb/xml-comparison'))
    parser.add_argument('--capture-only',action='store_true')
    parser.add_argument('--worker',type=Path,default=ROOT/'target/release/function_expression_worker')
    parser.add_argument('--data-bytes',type=int,default=64*1024*1024)
    args=parser.parse_args()
    if not args.directory.resolve().is_relative_to(Path('/tmp/mydb').resolve()): parser.error('--directory must be below /tmp/mydb')
    raise SystemExit(0 if (differential(args) if args.mode=='diff' else benchmark(args)) else 1)

if __name__=='__main__': main()
