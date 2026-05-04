# MapReduce — transações comerciais

Projeto Maven (Java 11, Hadoop 3.3.6) com oito jobs MapReduce sobre CSV (`;`, 10 colunas). Entrada padrão: **`operacoes_comerciais_inteira.csv`** na raiz deste diretório (mesmo nível do `pom.xml`).

## Preciso instalar o Hadoop?

- **Para rodar no IntelliJ** (modo local): não precisa do comando `hadoop` no PATH; o Maven traz as bibliotecas Java (`hadoop-client`). O projeto usa modo local (`mapreduce.framework.name=local`).
- **No Windows** o modo local precisa de **`tools\hadoop\bin\winutils.exe`** e **`tools\hadoop\bin\hadoop.dll`** (sem a DLL ocorre `UnsatisfiedLinkError` em `NativeIO$Windows.access0`). O projeto usa `hadoop.home.dir = <raiz>\tools\hadoop` quando **`HADOOP_HOME`** não está definido.
  - Na raiz de `mapreduce-trade-stats`:  
    `powershell -ExecutionPolicy Bypass -File scripts\download-winutils.ps1`  
    Se falhar (rede, proxy, TLS): **`scripts\download-winutils-zip.ps1`** ou guia passo a passo em **[`tools/hadoop/BAIXAR.md`](tools/hadoop/BAIXAR.md)** (browser, ZIP, `curl`).
  - **Alternativa:** **`HADOOP_HOME`** apontando para uma instalação Hadoop com `bin\winutils.exe` e `bin\hadoop.dll`.
  - Detalhes: [`tools/hadoop/README.md`](tools/hadoop/README.md).
- **Para `hadoop jar` num cluster** (laboratório): use a instalação Hadoop indicada pelo professor.

## Pré-requisitos

- JDK 11+
- Windows: `tools\hadoop\bin\winutils.exe` e `hadoop.dll` (ou `HADOOP_HOME` com ambos em `bin\`)
- Arquivo `operacoes_comerciais_inteira.csv` na raiz (ou caminho no 1.º argumento)

## Tratamento de dados

- **Cabeçalho:** linhas em que a coluna país é `Country` (case insensitive) ou o ano não é inteiro são ignoradas.
- **Linhas inválidas:** menos de 10 colunas após o split com `;` são ignoradas.
- **Brasil:** aceita `Brazil` e `Brasil` (comparação case insensitive).
- **Export (Q7):** compara o fluxo com `Export` em modo case insensitive.
- **Números:** `Price` e `Amount` aceitam vírgula ou ponto como separador decimal.

## Build

```powershell
cd mapreduce-trade-stats
mvn -DskipTests package
```

O JAR em `target/trade-mapreduce-1.0.0.jar` contém só o vosso código (shade); ao usar `hadoop jar`, o Hadoop da instalação fornece o resto. No IntelliJ o Maven coloca o `hadoop-client` no classpath automaticamente.

## Rodar tudo de uma vez (main único)

Classe **`com.trademr.RunAllJobs`**: executa os 8 jobs em sequência com modo local e as mesmas pastas `out/...` que os drivers individuais.

- **IntelliJ:** Run → Edit Configurations → **Main class** `com.trademr.RunAllJobs`, **Working directory** = pasta `mapreduce-trade-stats`, **Program arguments** = vazio (usa `operacoes_comerciais_inteira.csv` na raiz) ou `scripts\sample_operacoes_comerciais.csv`. Antes da primeira execução (ou após mudar código), corra **`mvn -DskipTests package`** para gerar `target/trade-mapreduce-1.0.0.jar` — o `LocalJobRunner` no Windows usa esse JAR para encontrar mappers/reducers. O código chama `System.load` em `hadoop.dll` **antes** de `new Configuration()` para evitar `UnsatisfiedLinkError` em `NativeIO$Windows.access0`. Se ainda falhar, em **VM options** experimente (caminho absoluto): `-Djava.library.path=C:\caminho\para\mapreduce-trade-stats\tools\hadoop\bin`
- **Um driver só (ex. Q1) no IntelliJ:** o `main` do driver usa `Configuration` padrão (adequado a cluster/YARN). Para modo ficheiro local, use na mesma configuração **VM options**: `-Dmapreduce.framework.name=local -Dfs.defaultFS=file:///` ou continue a usar `RunAllJobs`.
- **Maven / terminal (só Java, sem `hadoop`):**

```powershell
cd mapreduce-trade-stats
mvn -q -DskipTests exec:java -Dexec.mainClass=com.trademr.RunAllJobs -Dexec.args="scripts/sample_operacoes_comerciais.csv"
```

O `pom.xml` já inclui o `exec-maven-plugin`; também pode usar só o botão Run no IntelliJ.

## Execução com `hadoop jar` (cluster / laboratório)

Defina `$JAR` e `$IN` (entrada). Saídas vão para `out/...` (apagadas automaticamente pelos drivers se já existirem).

```powershell
$JAR = "target\trade-mapreduce-1.0.0.jar"
$IN  = "operacoes_comerciais_inteira.csv"

hadoop jar $JAR com.trademr.job1.Q1BrazilCountDriver $IN out/q1_brazil_count
hadoop jar $JAR com.trademr.job2.Q2TransactionsPerYearDriver $IN out/q2_per_year
hadoop jar $JAR com.trademr.job3.Q3TransactionsPerCategoryDriver $IN out/q3_per_category
hadoop jar $JAR com.trademr.job4.Q4TransactionsPerFlowDriver $IN out/q4_per_flow
hadoop jar $JAR com.trademr.job5.Q5BrazilAvgPriceDriver $IN out/q5_avg_price_brazil
hadoop jar $JAR com.trademr.job6.Q6Brazil2016MinMaxDriver $IN out/q6_minmax_price_brazil_2016
hadoop jar $JAR com.trademr.job7.Q7BrazilExportAvgDriver $IN out/q7_avg_price_brazil_export
hadoop jar $JAR com.trademr.job8.Q8MinMaxAmountPerYearCountryDriver $IN out/q8_minmax_amount_year_country
```

### Teste rápido com amostra

Use o CSV em [`scripts/sample_operacoes_comerciais.csv`](scripts/sample_operacoes_comerciais.csv):

```powershell
$IN = "scripts\sample_operacoes_comerciais.csv"
hadoop jar $JAR com.trademr.job1.Q1BrazilCountDriver $IN out/q1_sample
```

Compare com os arquivos em [`resultados/`](resultados/) — hoje eles refletem o esperado para a amostra; veja [`resultados/NOTA.txt`](resultados/NOTA.txt). Com o CSV completo, rode `collect-results.ps1` após os jobs.

## Consolidar saídas em `resultados/*.txt`

Após cada job, os resultados estão em `out/<pasta>/part-r-00000` (e possivelmente mais `part-r-*`).

```powershell
.\scripts\collect-results.ps1
```

O script concatena todos os `part-r-*` de cada pasta `out/q*` para `resultados/questao_1.txt` … `questao_8.txt`.

## Mapas de questões → classes

| Questão | Driver |
|--------|--------|
| 1 — Transações Brasil | `com.trademr.job1.Q1BrazilCountDriver` |
| 2 — Por ano | `com.trademr.job2.Q2TransactionsPerYearDriver` |
| 3 — Por categoria | `com.trademr.job3.Q3TransactionsPerCategoryDriver` |
| 4 — Por flow | `com.trademr.job4.Q4TransactionsPerFlowDriver` |
| 5 — Média preço (Brasil) / ano | `com.trademr.job5.Q5BrazilAvgPriceDriver` |
| 6 — Min/max preço Brasil 2016 | `com.trademr.job6.Q6Brazil2016MinMaxDriver` |
| 7 — Média preço Export Brasil / ano + Combiner | `com.trademr.job7.Q7BrazilExportAvgDriver` |
| 8 — Min/max Amount por ano e país | `com.trademr.job8.Q8MinMaxAmountPerYearCountryDriver` |

## Writables customizados

- `SumCountWritable` — soma e contagem (médias)
- `MinMaxPriceWritable` — mínimo e máximo de preço (Q6)
- `MinMaxAmountWritable` — mínimo e máximo de `Amount` (Q8)
- `YearCountryKey` — `WritableComparable` ano + país (Q8)

Não há formação de chaves compostas por concatenação de `String`; chaves/valores compostos usam os tipos acima.
