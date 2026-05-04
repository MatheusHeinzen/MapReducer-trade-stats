# Hadoop no Windows (winutils + hadoop.dll)

O **LocalJobRunner** (MapReduce no IntelliJ) no Windows usa código JNI (`NativeIO$Windows.access0`). É preciso **`winutils.exe`** e **`hadoop.dll`** na pasta `bin` de `hadoop.home.dir`.

Com **working directory** = raiz do módulo `mapreduce-trade-stats`:

`hadoop.home.dir` = `<raiz>\tools\hadoop`

Ficheiros:

```text
tools/hadoop/bin/winutils.exe
tools/hadoop/bin/hadoop.dll
```

Sem `hadoop.dll` aparece: `UnsatisfiedLinkError: NativeIO$Windows.access0`.

## Obter os ficheiros

Guia completo com várias opções (browser, ZIP, curl): **[BAIXAR.md](BAIXAR.md)**.

1. **Script** — `scripts\download-winutils.ps1` (URLs diretas; tenta dois espelhos).
2. **ZIP do GitHub** — se o script falhar: `scripts\download-winutils-zip.ps1`.
3. **Manual:** [cdarlint/winutils](https://github.com/cdarlint/winutils), pasta **`hadoop-3.3.6/bin/`**.

3. **Alternativa:** instalação Hadoop completa no Windows com **`HADOOP_HOME`** (deve incluir ambos em `bin\`).

Mais contexto: [WindowsProblems](https://wiki.apache.org/hadoop/WindowsProblems).
