package com.trademr.job6;

import com.trademr.writable.MinMaxPriceWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Q6MinMaxPriceReducer extends Reducer<IntWritable, MinMaxPriceWritable, Text, Text> {

  private static final Text OUT_KEY = new Text("brazil_2016_price_min_max");

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
    String line = acc.getMin() + "\t" + acc.getMax();
    context.write(OUT_KEY, new Text(line));
  }
}
