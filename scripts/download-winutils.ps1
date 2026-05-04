# Baixa winutils.exe e hadoop.dll (Hadoop 3.3.6). Se falhar, use download-winutils-zip.ps1 ou tools/hadoop/BAIXAR.md

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$destDir = Join-Path $root "tools\hadoop\bin"

# Dois espelhos: raw.githubusercontent e redirect github raw (algumas redes bloqueiam um deles)
$files = @(
  @{
    Name = "winutils.exe"
    Urls = @(
      "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/winutils.exe",
      "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe"
    )
  },
  @{
    Name = "hadoop.dll"
    Urls = @(
      "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/hadoop.dll",
      "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll"
    )
  }
)

[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
New-Item -ItemType Directory -Force -Path $destDir | Out-Null

foreach ($f in $files) {
  $dest = Join-Path $destDir $f.Name
  $ok = $false
  foreach ($url in $f.Urls) {
    Write-Host "A tentar $($f.Name) <- $url"
    try {
      Invoke-WebRequest -Uri $url -OutFile $dest -UseBasicParsing -UserAgent "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
      if ((Test-Path $dest) -and (Get-Item $dest).Length -gt 0) {
        $ok = $true
        break
      }
    } catch {
      Write-Host "  falhou: $($_.Exception.Message)"
    }
  }
  if (-not $ok) {
    Write-Host ""
    Write-Host "Download direto falhou. Alternativas:"
    Write-Host "  1) powershell -ExecutionPolicy Bypass -File scripts\download-winutils-zip.ps1"
    Write-Host "  2) Leia tools\hadoop\BAIXAR.md (browser, curl, ZIP manual)"
    throw "Nao foi possivel obter $($f.Name)"
  }
  Write-Host "OK: $dest"
}

Write-Host "Concluido."
