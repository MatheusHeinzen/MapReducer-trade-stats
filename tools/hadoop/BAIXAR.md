# Como obter `winutils.exe` e `hadoop.dll` no Windows

Rode
```powershell
powershell -ExecutionPolicy Bypass -File scripts\download-winutils.ps1
```

Coloque os **dois** ficheiros em:

`mapreduce-trade-stats/tools/hadoop/bin/`

Versão alinhada ao projeto: **Hadoop 3.3.6** (pasta `hadoop-3.3.6` no repositório).

---

## 1) Pelo site do GitHub (mais fiável se script falhar)

1. Abra no navegador:  
   **https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.6/bin**
2. Clique em **`winutils.exe`** → botão **Download** (ou **Raw** e depois *Guardar como…*).
3. Repita para **`hadoop.dll`**.
4. Mova os ficheiros para `tools\hadoop\bin\` na raiz do módulo `mapreduce-trade-stats`.

Links diretos “Raw” (copiar e colar na barra do browser ou noutro gestor de downloads):

- https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe  
- https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll  

---

## 2) ZIP do repositório (útil com proxy/firewall)

1. Abra: **https://github.com/cdarlint/winutils/archive/refs/heads/master.zip**
2. Guarde o ZIP e extraia.
3. Dentro da pasta extraída vá a **`winutils-master\hadoop-3.3.6\bin\`**.
4. Copie **`winutils.exe`** e **`hadoop.dll`** para `tools\hadoop\bin\`.

Ou na raiz do módulo:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\download-winutils-zip.ps1
```

---

## 3) `curl` (Windows 10/11)

Na pasta `mapreduce-trade-stats`:

```powershell
mkdir tools\hadoop\bin -Force
curl.exe -fsSL -o tools\hadoop\bin\winutils.exe "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe"
curl.exe -fsSL -o tools\hadoop\bin\hadoop.dll "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll"
```

---

## 4) Instalação Hadoop completa + `HADOOP_HOME`

Se já tiver (ou puder instalar) o Hadoop 3.3.x para Windows com `bin\winutils.exe` e `bin\hadoop.dll`, defina a variável de ambiente **`HADOOP_HOME`** para essa pasta. O projeto deixa de usar `tools\hadoop` automaticamente.

---

## Problemas comuns

| Sintoma | O que tentar |
|--------|----------------|
| `Invoke-WebRequest` falha | Use o **ZIP** (secção 2) ou **curl** (secção 3). |
| TLS / HTTPS | Atualize o Windows; ou use o download pelo **browser**. |
| Proxy corporativo | Configure proxy no sistema ou baixe o ZIP noutra rede e copie os ficheiros. |
| Ainda dá `UnsatisfiedLinkError` | Confirme que **`hadoop.dll`** e **`winutils.exe`** estão na **mesma** pasta `bin` e que o IntelliJ tem **Working directory** = pasta `mapreduce-trade-stats`. |

Documentação geral: https://wiki.apache.org/hadoop/WindowsProblems  
