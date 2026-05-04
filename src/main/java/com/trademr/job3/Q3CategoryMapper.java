package com.trademr.job3;

import com.trademr.common.CsvLineParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Q3CategoryMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

  private final Text category = new Text();
  private final LongWritable one = new LongWritable(1);

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] cols = CsvLineParser.split(value.toString());
    if (!CsvLineParser.isValidRow(cols) || CsvLineParser.isHeaderLine(cols)) {
      return;
    }
    String cat = CsvLineParser.trim(cols[CsvLineParser.COL_CATEGORY]);
    if (cat.isEmpty()) {
      return;
    }
    category.set(cat);
    context.write(category, one);
  }
}
