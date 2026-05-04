package com.trademr.job6;

import com.trademr.writable.MinMaxPriceWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Q6MinMaxPriceCombiner extends Reducer<IntWritable, MinMaxPriceWritable, IntWritable, MinMaxPriceWritable> {

  @Override
  protected void reduce(IntWritable key, Iterable<MinMaxPriceWritable> values, Context context)
      throws IOException, InterruptedException {
    MinMaxPriceWritable acc = new MinMaxPriceWritable();
    for (MinMaxPriceWritable v : values) {
      acc.merge(v);
    }
    if (acc.isEmpty()) {
      return;
    }
    context.write(key, acc);
  }
}
