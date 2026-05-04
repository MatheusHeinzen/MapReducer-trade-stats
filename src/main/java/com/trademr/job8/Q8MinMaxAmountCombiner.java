package com.trademr.job8;

import com.trademr.writable.MinMaxAmountWritable;
import com.trademr.writable.YearCountryKey;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Q8MinMaxAmountCombiner extends Reducer<YearCountryKey, MinMaxAmountWritable, YearCountryKey, MinMaxAmountWritable> {

  @Override
  protected void reduce(YearCountryKey key, Iterable<MinMaxAmountWritable> values, Context context)
      throws IOException, InterruptedException {
    MinMaxAmountWritable acc = new MinMaxAmountWritable();
    for (MinMaxAmountWritable v : values) {
      acc.merge(v);
    }
    if (acc.isEmpty()) {
      return;
    }
    context.write(key, acc);
  }
}
