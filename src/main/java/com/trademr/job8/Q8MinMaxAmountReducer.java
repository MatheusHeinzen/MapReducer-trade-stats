package com.trademr.job8;

import com.trademr.writable.MinMaxAmountWritable;
import com.trademr.writable.YearCountryKey;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Q8MinMaxAmountReducer extends Reducer<YearCountryKey, MinMaxAmountWritable, Text, Text> {

  private final Text outKey = new Text();
  private final Text outVal = new Text();

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
    outKey.set(key.getYear() + "\t" + key.getCountry().toString());
    outVal.set(acc.getMin() + "\t" + acc.getMax());
    context.write(outKey, outVal);
  }
}
