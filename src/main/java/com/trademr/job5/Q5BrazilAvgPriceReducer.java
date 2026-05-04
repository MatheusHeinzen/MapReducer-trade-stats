package com.trademr.job5;

import com.trademr.writable.SumCountWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Q5BrazilAvgPriceReducer extends Reducer<IntWritable, SumCountWritable, IntWritable, DoubleWritable> {

  private final DoubleWritable avg = new DoubleWritable();

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
    avg.set(acc.getSum() / acc.getCount());
    context.write(key, avg);
  }
}
