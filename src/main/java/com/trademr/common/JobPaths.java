package com.trademr.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public final class JobPaths {

  public static final String DEFAULT_INPUT = "operacoes_comerciais_inteira.csv";

  private JobPaths() {}

  public static Path inputPath(String[] args) {
    String in = args.length > 0 ? args[0] : DEFAULT_INPUT;
    return new Path(in);
  }

  public static Path outputPath(String[] args, int argIndex, String defaultOut) {
    String out = args.length > argIndex ? args[argIndex] : defaultOut;
    return new Path(out);
  }

  public static void deleteIfExists(Configuration conf, Path path) throws IOException {
    MrLocal.bootstrapWindowsBeforeHadoop();
    FileSystem fs = path.getFileSystem(conf);
    if (fs.exists(path)) {
      fs.delete(path, true);
    }
  }
}
