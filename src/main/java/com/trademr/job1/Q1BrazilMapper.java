package com.trademr.job1;

import com.trademr.common.CsvLineParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Q1BrazilMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable> {

  private static final IntWritable OUT_KEY = new IntWritable(0);
  private final LongWritable one = new LongWritable(1);

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] cols = CsvLineParser.split(value.toString());
    if (!CsvLineParser.isValidRow(cols) || CsvLineParser.isHeaderLine(cols)) {
      return;
    }
    if (CsvLineParser.isBrazil(cols[CsvLineParser.COL_COUNTRY])) {
      context.write(OUT_KEY, one);
    }
  }
}
