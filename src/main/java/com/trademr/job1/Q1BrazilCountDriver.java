package com.trademr.job1;

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

public final class Q1BrazilCountDriver {

  private Q1BrazilCountDriver() {}

  public static int run(String[] args, Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
    Path in = JobPaths.inputPath(args);
    Path out = JobPaths.outputPath(args, 1, "out/q1_brazil_count");
    JobPaths.deleteIfExists(conf, out);

    Job job = Job.getInstance(conf, "Q1 Brazil transaction count");
    MrLocal.prepareJobJar(job, Q1BrazilCountDriver.class);

    job.setMapperClass(Q1BrazilMapper.class);
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
