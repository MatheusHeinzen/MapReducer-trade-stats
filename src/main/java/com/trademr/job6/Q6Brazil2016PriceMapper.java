package com.trademr.job6;

import com.trademr.common.CsvLineParser;
import com.trademr.writable.MinMaxPriceWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Q6Brazil2016PriceMapper extends Mapper<LongWritable, Text, IntWritable, MinMaxPriceWritable> {

  private static final int YEAR_2016 = 2016;
  private static final IntWritable OUT_KEY = new IntWritable(0);

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] cols = CsvLineParser.split(value.toString());
    if (!CsvLineParser.isValidRow(cols) || CsvLineParser.isHeaderLine(cols)) {
      return;
    }
    if (!CsvLineParser.isBrazil(cols[CsvLineParser.COL_COUNTRY])) {
      return;
    }
    Integer y = CsvLineParser.parseYear(cols[CsvLineParser.COL_YEAR]);
    if (y == null || y != YEAR_2016) {
      return;
    }
    Double price = CsvLineParser.parseDoubleField(cols[CsvLineParser.COL_PRICE]);
    if (price == null) {
      return;
    }
    context.write(OUT_KEY, new MinMaxPriceWritable(price));
  }
}
