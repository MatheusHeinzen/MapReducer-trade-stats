package com.trademr.job2;

import com.trademr.common.CsvLineParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Q2YearMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable> {

  private final IntWritable year = new IntWritable();
  private final LongWritable one = new LongWritable(1);

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] cols = CsvLineParser.split(value.toString());
    if (!CsvLineParser.isValidRow(cols) || CsvLineParser.isHeaderLine(cols)) {
      return;
    }
    Integer y = CsvLineParser.parseYear(cols[CsvLineParser.COL_YEAR]);
    if (y == null) {
      return;
    }
    year.set(y);
    context.write(year, one);
  }
}
