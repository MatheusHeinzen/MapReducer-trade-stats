# Executar a partir da raiz do projeto (mapreduce-trade-stats)
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$map = @(
  @{ out = "out/q1_brazil_count"; dest = "resultados/questao_1.txt" },
  @{ out = "out/q2_per_year"; dest = "resultados/questao_2.txt" },
  @{ out = "out/q3_per_category"; dest = "resultados/questao_3.txt" },
  @{ out = "out/q4_per_flow"; dest = "resultados/questao_4.txt" },
  @{ out = "out/q5_avg_price_brazil"; dest = "resultados/questao_5.txt" },
  @{ out = "out/q6_minmax_price_brazil_2016"; dest = "resultados/questao_6.txt" },
  @{ out = "out/q7_avg_price_brazil_export"; dest = "resultados/questao_7.txt" },
  @{ out = "out/q8_minmax_amount_year_country"; dest = "resultados/questao_8.txt" }
)

New-Item -ItemType Directory -Force -Path "resultados" | Out-Null

foreach ($m in $map) {
  $dir = $m.out
  if (-not (Test-Path $dir)) {
    Write-Warning "Pasta inexistente (execute o job antes): $dir"
    continue
  }
  $parts = Get-ChildItem -Path $dir -Filter "part-r-*" | Sort-Object Name
  if ($parts.Count -eq 0) {
    Write-Warning "Nenhum part-r-* em $dir"
    continue
  }
  $sb = New-Object System.Text.StringBuilder
  foreach ($p in $parts) {
    [void]$sb.Append((Get-Content $p.FullName -Raw))
  }
  $text = $sb.ToString().TrimEnd()
  [System.IO.File]::WriteAllText((Join-Path $root $m.dest), $text + "`n", [System.Text.UTF8Encoding]::new($false))
  Write-Host "OK $($m.dest)"
}
