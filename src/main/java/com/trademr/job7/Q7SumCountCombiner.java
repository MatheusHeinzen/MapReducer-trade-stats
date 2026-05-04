package com.trademr.job7;

import com.trademr.writable.SumCountWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Q7SumCountCombiner extends Reducer<IntWritable, SumCountWritable, IntWritable, SumCountWritable> {

  @Override
  protected void reduce(IntWritable key, Iterable<SumCountWritable> values, Context context)
      throws IOException, InterruptedException {
    SumCountWritable acc = new SumCountWritable(0D, 0L);
    for (SumCountWritable v : values) {
      acc.merge(v);
    }
    if (acc.getCount() == 0) {
      return;
    }
    context.write(key, acc);
  }
}
