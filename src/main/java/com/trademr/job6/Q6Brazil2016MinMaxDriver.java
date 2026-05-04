package com.trademr.job6;

import com.trademr.common.JobPaths;
import com.trademr.common.MrLocal;
import com.trademr.writable.MinMaxPriceWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public final class Q6Brazil2016MinMaxDriver {

  private Q6Brazil2016MinMaxDriver() {}

  public static int run(String[] args, Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
    Path in = JobPaths.inputPath(args);
    Path out = JobPaths.outputPath(args, 1, "out/q6_minmax_price_brazil_2016");
    JobPaths.deleteIfExists(conf, out);

    Job job = Job.getInstance(conf, "Q6 Min/max Price Brazil 2016");
    MrLocal.prepareJobJar(job, Q6Brazil2016MinMaxDriver.class);

    job.setMapperClass(Q6Brazil2016PriceMapper.class);
    job.setCombinerClass(Q6MinMaxPriceCombiner.class);
    job.setReducerClass(Q6MinMaxPriceReducer.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(MinMaxPriceWritable.class);
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
