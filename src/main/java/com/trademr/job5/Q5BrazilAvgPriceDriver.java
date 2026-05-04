package com.trademr.job5;

import com.trademr.common.JobPaths;
import com.trademr.common.MrLocal;
import com.trademr.writable.SumCountWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public final class Q5BrazilAvgPriceDriver {

  private Q5BrazilAvgPriceDriver() {}

  public static int run(String[] args, Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
    Path in = JobPaths.inputPath(args);
    Path out = JobPaths.outputPath(args, 1, "out/q5_avg_price_brazil");
    JobPaths.deleteIfExists(conf, out);

    Job job = Job.getInstance(conf, "Q5 Average price per year (Brazil)");
    MrLocal.prepareJobJar(job, Q5BrazilAvgPriceDriver.class);

    job.setMapperClass(Q5BrazilAvgPriceMapper.class);
    job.setReducerClass(Q5BrazilAvgPriceReducer.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(SumCountWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(DoubleWritable.class);

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
