#Requires -Version 5.1
#Requires -RunAsAdministrator

param(
    [ValidatePattern('^[a-z][a-z0-9_-]{0,19}$')]
    [string]$UserName = 'mydbtest'
)

$ErrorActionPreference = 'Stop'

if (-not [Environment]::Is64BitProcess) {
    throw 'Run this script in 64-bit Windows PowerShell.'
}
if (Get-LocalUser -Name $UserName -ErrorAction SilentlyContinue) {
    throw "Account '$UserName' already exists. Choose another name; its password was not changed."
}

$Password = Read-Host "Enter a password for $UserName" -AsSecureString
if ($Password.Length -eq 0) {
    $Password.Dispose()
    throw 'A non-empty password is required.'
}

try {
    $capability = Get-WindowsCapability -Online -Name 'OpenSSH.Server~~~~0.0.1.0'
    if ($capability.State -ne 'Installed') {
        $installed = Add-WindowsCapability -Online -Name 'OpenSSH.Server~~~~0.0.1.0'
        if ($installed.RestartNeeded) {
            throw 'Restart Windows to finish installing OpenSSH, then run this script again.'
        }
    }
    Set-Service -Name sshd -StartupType Automatic
    Start-Service -Name sshd

    $account = New-LocalUser -Name $UserName -Password $Password `
        -Description 'mydb Windows SSH test account'
}
finally {
    $Password.Dispose()
}

# Resolve the built-in Users group by SID for localized Windows editions.
$users = Get-LocalGroup -SID 'S-1-5-32-545'
$memberSids = @(Get-LocalGroupMember -Group $users.Name | ForEach-Object { $_.SID.Value })
if ($memberSids -notcontains $account.SID.Value) {
    Add-LocalGroupMember -Group $users.Name -Member $account
}

# Some installations restrict SSH access to this optional local group.
$sshUsers = Get-LocalGroup -Name 'OpenSSH Users' -ErrorAction SilentlyContinue
if ($sshUsers) {
    Add-LocalGroupMember -Group $sshUsers.Name -Member $account
}

$sshd = Join-Path $env:WINDIR 'System32\OpenSSH\sshd.exe'
$config = Join-Path $env:ProgramData 'ssh\sshd_config'
$connection = "user=$UserName,host=$env:COMPUTERNAME,addr=127.0.0.1"
$effective = @(& $sshd -T -f $config -C $connection)
if ($LASTEXITCODE -ne 0) {
    throw "Cannot evaluate SSH configuration: $config"
}

if (($effective -notcontains 'passwordauthentication yes') -or
    (($effective -notcontains 'authenticationmethods any') -and
     ($effective -notcontains 'authenticationmethods password'))) {
    $original = [IO.File]::ReadAllText($config)
    $backup = "$config.mydb-$([guid]::NewGuid().ToString('N')).bak"
    Copy-Item -LiteralPath $config -Destination $backup
    $block = "`r`nMatch User $UserName`r`n    PasswordAuthentication yes`r`n    AuthenticationMethods any`r`n`r`n"
    # Global directives must remain before all Match sections. Place the
    # account-specific section first so broader later matches cannot override it.
    $firstMatch = [regex]::Match($original, '(?im)^[ \t]*Match[ \t]+')
    if ($firstMatch.Success) {
        $updated = $original.Insert($firstMatch.Index, $block)
    }
    else {
        $updated = $original + $block
    }
    [IO.File]::WriteAllText($config, $updated, [Text.UTF8Encoding]::new($false))
    & $sshd -t -f $config
    if ($LASTEXITCODE -ne 0) {
        Copy-Item -LiteralPath $backup -Destination $config -Force
        throw "SSH configuration rejected; restored the backup: $backup"
    }
    Restart-Service -Name sshd
    $effective = @(& $sshd -T -f $config -C $connection)
    if ($LASTEXITCODE -ne 0 -or $effective -notcontains 'passwordauthentication yes') {
        throw 'The effective SSH policy still rejects password authentication.'
    }
    Write-Host "SSH configuration backup: $backup"
}

# Open the actual configured SSH ports without changing existing allow/deny users.
$ports = @($effective | Where-Object { $_ -match '^port [0-9]+$' } |
    ForEach-Object { [int](($_ -split ' ')[1]) } | Select-Object -Unique)
if ($ports.Count -eq 0) {
    throw 'No SSH listening port was found in the effective configuration.'
}
foreach ($port in $ports) {
    $ruleName = "MyDB-SSH-$port"
    if (Get-NetFirewallRule -Name $ruleName -ErrorAction SilentlyContinue) {
        Enable-NetFirewallRule -Name $ruleName | Out-Null
    }
    else {
        New-NetFirewallRule -Name $ruleName -DisplayName "mydb SSH TCP $port" `
            -Enabled True -Direction Inbound -Protocol TCP -LocalPort $port `
            -Action Allow | Out-Null
    }
}

$restrictions = @($effective | Where-Object { $_ -match '^(allowusers|denyusers|allowgroups|denygroups) ' })
if ($restrictions.Count -gt 0) {
    Write-Warning "Existing SSH access restrictions remain active: $($restrictions -join '; ')"
}

Write-Host "Created standard account: $UserName"
Write-Host "From your Mac: ssh -p $($ports[0]) ${UserName}@<Windows-IP>"
Get-NetIPAddress -AddressFamily IPv4 |
    Where-Object { $_.IPAddress -notmatch '^(127\.|169\.254\.)' } |
    Select-Object InterfaceAlias, IPAddress
