#Requires -Version 5.1
param(
    [Parameter(Mandatory=$true)][string]$TestBinary,
    [Parameter(Mandatory=$true)][string]$WorkDirectory,
    [string]$SeedBinary,
    [string]$Filter = '',
    [string]$TestList,
    [ValidateRange(1,16)][int]$Jobs = 4,
    [ValidateRange(10,3600)][int]$TimeoutSeconds = 600
)

$ErrorActionPreference='Stop'
$ProgressPreference='SilentlyContinue'
[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
$TestBinary=(Resolve-Path -LiteralPath $TestBinary).Path
$WorkDirectory=[IO.Path]::GetFullPath($WorkDirectory)
New-Item -ItemType Directory -Force -Path $WorkDirectory | Out-Null
if(Test-Path 'W:\'){throw 'W: already exists; refusing to replace its mapping'}
& subst.exe W: $WorkDirectory
if($LASTEXITCODE -ne 0){throw 'Cannot create isolated W: test drive'}

try {
    Set-Location 'W:\'
    New-Item -ItemType Directory -Force -Path '\tmp\mydb' | Out-Null
    $env:MYDB_TEST_TMP_DIR='W:\tmp\mydb'
    $env:TEMP=$env:MYDB_TEST_TMP_DIR
    $env:TMP=$env:TEMP
    $env:TMPDIR=$env:TEMP
    if($SeedBinary) {
        $env:NEXTEST_ENV=Join-Path $WorkDirectory 'seed.env'
        $env:NEXTEST_RUN_ID=[guid]::NewGuid().ToString()
        $env:MYDB_TEST_SEED_ROOT='W:\tmp\mydb\seeds'
        [IO.File]::WriteAllText($env:NEXTEST_ENV,'')
        & cmd.exe /d /c ('"'+$SeedBinary+'" > "'+$WorkDirectory+'\seed.log" 2>&1')
        if($LASTEXITCODE -ne 0){throw 'Test seed creation failed; inspect seed.log'}
        foreach($line in [IO.File]::ReadAllLines($env:NEXTEST_ENV)) {
            $parts=$line -split '=',2
            [Environment]::SetEnvironmentVariable($parts[0],$parts[1],'Process')
        }
    }
    Add-Type -TypeDefinition @'
using System;
using System.IO;
using System.Diagnostics;
using System.Linq;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
public static class MydbNativeTests {
    static readonly object Gate = new object();
    public static int Run(string exe, string work, string filter, string testList, int jobs, int timeout) {
        var listing = new Process();
        listing.StartInfo = new ProcessStartInfo(exe, "--list --format terse") {
            UseShellExecute=false, RedirectStandardOutput=true, RedirectStandardError=true,
            CreateNoWindow=true, WorkingDirectory="W:\\"
        };
        listing.Start();
        string names = listing.StandardOutput.ReadToEnd();
        string listingErrors = listing.StandardError.ReadToEnd();
        listing.WaitForExit();
        if(listing.ExitCode != 0) throw new Exception(listingErrors);
        var selected = String.IsNullOrEmpty(testList) ? null : new HashSet<string>(File.ReadAllLines(testList),StringComparer.Ordinal);
        var tests = names.Split(new[] {'\r','\n'},StringSplitOptions.RemoveEmptyEntries)
            .Where(s=>s.EndsWith(": test"))
            .Select(s=>s.Substring(0,s.Length-6))
            .Where(s=>filter.Length==0 || s.IndexOf(filter,StringComparison.Ordinal)>=0)
            .Where(s=>selected==null || selected.Contains(s)).ToArray();
        if(tests.Length==0) throw new Exception("No tests matched the requested selection");
        if(selected!=null && selected.Except(tests).Any()) throw new Exception("Test list contains names not present in the executable or excluded by the filter");
        Directory.CreateDirectory(Path.Combine(work,"logs"));
        int passed=0, failed=0, ignored=0, finished=0, nextLog=0;
        Console.WriteLine("Selected {0} isolated tests, workers={1}",tests.Length,jobs);
        using(var summary = new StreamWriter(Path.Combine(work,"results.tsv"),false,new System.Text.UTF8Encoding(false))) {
            Parallel.ForEach(tests,new ParallelOptions {MaxDegreeOfParallelism=jobs},name=>{
                var p = new Process();
                p.StartInfo = new ProcessStartInfo(exe,"--exact "+name+" --nocapture --test-threads=1") {
                    UseShellExecute=false, RedirectStandardOutput=true, RedirectStandardError=true,
                    CreateNoWindow=true, WorkingDirectory="W:\\"
                };
                string state="FAIL", output="", error="";
                var elapsed=Stopwatch.StartNew();
                try {
                    p.Start();
                    var stdout=p.StandardOutput.ReadToEndAsync();
                    var stderr=p.StandardError.ReadToEndAsync();
                    if(!p.WaitForExit(timeout*1000)) {p.Kill();p.WaitForExit();state="TIMEOUT";}
                    else state=p.ExitCode==0?"PASS":"FAIL";
                    if(Task.WaitAll(new Task[] {stdout,stderr},5000)) {output=stdout.Result;error=stderr.Result;}
                    else {state="LEAK";error="Output handles remained open after test exit";}
                    if(state=="PASS" && output.Contains("1 ignored")) state="IGNORED";
                } catch(Exception ex) {error=ex.ToString();}
                finally {p.Dispose();}
                string path=Path.Combine(work,"logs",Interlocked.Increment(ref nextLog).ToString("D6")+".log");
                File.WriteAllText(path,output+error,new System.Text.UTF8Encoding(false));
                lock(Gate) {
                    if(state=="PASS") passed++;else if(state=="IGNORED") ignored++;else failed++;
                    finished++;
                    summary.WriteLine("{0}\t{1}\t{2}\t{3}",state,name,elapsed.ElapsedMilliseconds,path);
                    summary.Flush();
                    if(state!="PASS" && state!="IGNORED" || finished%100==0)
                        Console.WriteLine("{0}/{1} passed={2} failed={3} ignored={4} {5} {6}",finished,tests.Length,passed,failed,ignored,state,name);
                }
            });
        }
        Console.WriteLine("SUMMARY total={0} passed={1} failed={2} ignored={3}",tests.Length,passed,failed,ignored);
        return failed==0?0:1;
    }
}
'@
    $result=[MydbNativeTests]::Run($TestBinary,$WorkDirectory,$Filter,$TestList,$Jobs,$TimeoutSeconds)
    exit $result
} finally {
    Set-Location $env:SystemDrive\
    & subst.exe W: /D
}
