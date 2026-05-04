package com.trademr.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

/**
 * Executa MapReduce no modo local (sem cluster/YARN), usando o sistema de ficheiros local.
 * Útil para correr a partir do IntelliJ ou da linha de comandos sem {@code hadoop jar}.
 */
public final class MrLocal {

  /** Nome do JAR em {@code target/} após {@code mvn package} (deve coincidir com o pom). */
  public static final String PACKAGED_JAR_NAME = "trade-mapreduce-1.0.0.jar";

  private static volatile boolean windowsHadoopDllLoaded = false;

  static {
    System.setProperty("io.native.lib.available", "false");
  }

  private MrLocal() {}

  /**
   * Chamar antes do primeiro {@code new Configuration()} no Windows.
   * Carrega {@code hadoop.dll} explicitamente; caso contrário o JNI {@code NativeIO$Windows.access0} falha.
   */
  public static void bootstrapWindowsBeforeHadoop() throws IOException {
    prepareHadoopHomeForLocalRuns();
    verifyWinutilsIfWindows();
    String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    if (!os.contains("win")) {
      return;
    }
    if (windowsHadoopDllLoaded) {
      return;
    }
    synchronized (MrLocal.class) {
      if (windowsHadoopDllLoaded) {
        return;
      }
      String home = System.getProperty("hadoop.home.dir");
      Path dll = Paths.get(home, "bin", "hadoop.dll").toAbsolutePath().normalize();
      if (!Files.isRegularFile(dll)) {
        throw new IOException("hadoop.dll nao encontrado: " + dll);
      }
      System.load(dll.toString());
      windowsHadoopDllLoaded = true;
    }
  }

  public static void apply(Configuration conf) {
    conf.set("mapreduce.framework.name", "local");
    conf.set("fs.defaultFS", "file:///");
    conf.set("mapreduce.app-submission.cross-platform", "true");
    // No Windows o MapReduce local ainda usa NativeIO$Windows (JNI) para staging — precisa de hadoop.dll em bin/.
    conf.setBoolean("io.native.lib.available", false);
  }

  /**
   * LocalJobRunner precisa do caminho do JAR com as classes do utilizador; no IntelliJ,
   * {@link Job#setJarByClass(Class)} por vezes não resolve. Usa {@code target/trade-mapreduce-*.jar} se existir.
   */
  public static void prepareJobJar(Job job, Class<?> jarClass) {
    job.setJarByClass(jarClass);
    Path jar = Paths.get(System.getProperty("user.dir"), "target", PACKAGED_JAR_NAME).toAbsolutePath().normalize();
    if (Files.isRegularFile(jar)) {
      job.setJar(jar.toString());
    }
  }

  /**
   * Garante {@code hadoop.home.dir} antes de qualquer {@code FileSystem} no Windows.
   * Ordem: propriedade JVM já definida → variável {@code HADOOP_HOME} → pasta {@code tools/hadoop}
   * sob o diretório de trabalho atual (ex.: raiz do módulo no IntelliJ).
   */
  public static void prepareHadoopHomeForLocalRuns() {
    String existing = System.getProperty("hadoop.home.dir");
    if (existing != null && !existing.isBlank()) {
      return;
    }
    String env = System.getenv("HADOOP_HOME");
    if (env != null && !env.isBlank()) {
      System.setProperty("hadoop.home.dir", env.trim());
      return;
    }
    String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    if (os.contains("win")) {
      Path home = Paths.get(System.getProperty("user.dir"), "tools", "hadoop").toAbsolutePath().normalize();
      System.setProperty("hadoop.home.dir", home.toString());
    }
  }

  /**
   * No Windows, o Hadoop exige {@code %HADOOP_HOME%\bin\winutils.exe} (ou o equivalente sob {@code hadoop.home.dir}).
   */
  public static void verifyWinutilsIfWindows() throws IOException {
    String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    if (!os.contains("win")) {
      return;
    }
    String home = System.getProperty("hadoop.home.dir");
    if (home == null || home.isBlank()) {
      throw new IOException(
          "hadoop.home.dir não está definido. Defina HADOOP_HOME ou corra a partir da raiz do projeto "
              + "(tools/hadoop será usado automaticamente).");
    }
    Path bin = Paths.get(home, "bin");
    Path winutils = bin.resolve("winutils.exe");
    Path hadoopDll = bin.resolve("hadoop.dll");
    if (!Files.isRegularFile(winutils)) {
      throw new IOException(
          "Falta: " + winutils + System.lineSeparator()
              + "Rode: powershell -ExecutionPolicy Bypass -File scripts\\download-winutils.ps1"
              + System.lineSeparator()
              + "https://wiki.apache.org/hadoop/WindowsProblems");
    }
    if (!Files.isRegularFile(hadoopDll)) {
      throw new IOException(
          "Falta: " + hadoopDll + " (obrigatório no Windows para NativeIO — evita UnsatisfiedLinkError em access0)."
              + System.lineSeparator()
              + "O mesmo script descarrega winutils.exe e hadoop.dll para tools/hadoop/bin/."
              + System.lineSeparator()
              + "Manual: https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.6/bin");
    }
  }
}
