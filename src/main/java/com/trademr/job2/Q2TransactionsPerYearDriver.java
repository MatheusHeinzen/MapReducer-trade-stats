package com.trademr.job2;

import com.trademr.common.JobPaths;
import com.trademr.common.MrLocal;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

import java.io.IOException;

public final class Q2TransactionsPerYearDriver {

  private Q2TransactionsPerYearDriver() {}

  public static int run(String[] args, Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
    Path in = JobPaths.inputPath(args);
    Path out = JobPaths.outputPath(args, 1, "out/q2_per_year");
    JobPaths.deleteIfExists(conf, out);

    Job job = Job.getInstance(conf, "Q2 Transactions per year");
    MrLocal.prepareJobJar(job, Q2TransactionsPerYearDriver.class);

    job.setMapperClass(Q2YearMapper.class);
    job.setCombinerClass(LongSumReducer.class);
    job.setReducerClass(LongSumReducer.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(LongWritable.class);

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
