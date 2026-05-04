# Baixa o repositório cdarlint/winutils como ZIP e copia hadoop-3.3.6/bin para tools/hadoop/bin.
# Útil quando raw.githubusercontent.com ou Invoke-WebRequest falham (proxy, TLS, etc.)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$destBin = Join-Path $root "tools\hadoop\bin"
$zipUrl = "https://github.com/cdarlint/winutils/archive/refs/heads/master.zip"
$tmp = Join-Path $env:TEMP ("winutils-" + [Guid]::NewGuid().ToString("n"))
$zipFile = Join-Path $tmp "winutils-master.zip"

New-Item -ItemType Directory -Force -Path $tmp | Out-Null
New-Item -ItemType Directory -Force -Path $destBin | Out-Null

try {
  [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
  Write-Host "A descarregar ZIP de GitHub..."
  Invoke-WebRequest -Uri $zipUrl -OutFile $zipFile -UseBasicParsing
  Write-Host "A extrair..."
  Expand-Archive -Path $zipFile -DestinationPath $tmp -Force
  $srcBin = Join-Path $tmp "winutils-master\hadoop-3.3.6\bin"
  if (-not (Test-Path $srcBin)) {
    throw "Pasta esperada nao encontrada: $srcBin"
  }
  Copy-Item -Path (Join-Path $srcBin "winutils.exe") -Destination $destBin -Force
  Copy-Item -Path (Join-Path $srcBin "hadoop.dll") -Destination $destBin -Force
  Write-Host "OK. Ficheiros em $destBin"
} finally {
  Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
}
