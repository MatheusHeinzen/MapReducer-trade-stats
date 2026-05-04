package com.trademr.job4;

import com.trademr.common.JobPaths;
import com.trademr.common.MrLocal;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

import java.io.IOException;

public final class Q4TransactionsPerFlowDriver {

  private Q4TransactionsPerFlowDriver() {}

  public static int run(String[] args, Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
    Path in = JobPaths.inputPath(args);
    Path out = JobPaths.outputPath(args, 1, "out/q4_per_flow");
    JobPaths.deleteIfExists(conf, out);

    Job job = Job.getInstance(conf, "Q4 Transactions per flow");
    MrLocal.prepareJobJar(job, Q4TransactionsPerFlowDriver.class);

    job.setMapperClass(Q4FlowMapper.class);
    job.setCombinerClass(LongSumReducer.class);
    job.setReducerClass(LongSumReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(Text.class);
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
