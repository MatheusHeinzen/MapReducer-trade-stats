package com.trademr.job8;

import com.trademr.common.JobPaths;
import com.trademr.common.MrLocal;
import com.trademr.writable.MinMaxAmountWritable;
import com.trademr.writable.YearCountryKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public final class Q8MinMaxAmountPerYearCountryDriver {

  private Q8MinMaxAmountPerYearCountryDriver() {}

  public static int run(String[] args, Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
    Path in = JobPaths.inputPath(args);
    Path out = JobPaths.outputPath(args, 1, "out/q8_minmax_amount_year_country");
    JobPaths.deleteIfExists(conf, out);

    Job job = Job.getInstance(conf, "Q8 Min/max Amount per year and country");
    MrLocal.prepareJobJar(job, Q8MinMaxAmountPerYearCountryDriver.class);

    job.setMapperClass(Q8YearCountryAmountMapper.class);
    job.setCombinerClass(Q8MinMaxAmountCombiner.class);
    job.setReducerClass(Q8MinMaxAmountReducer.class);

    job.setMapOutputKeyClass(YearCountryKey.class);
    job.setMapOutputValueClass(MinMaxAmountWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, in);
    FileOutputFormat.setOutputPath(job, out);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    MrLocal.bootstrapWindowsBeforeHadoop();
    return run(args, new Configuration());
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    System.exit(run(args));
  }
}
