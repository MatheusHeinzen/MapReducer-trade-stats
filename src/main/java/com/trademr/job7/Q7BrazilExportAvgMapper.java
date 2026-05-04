package com.trademr.job7;

import com.trademr.common.CsvLineParser;
import com.trademr.writable.SumCountWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Q7BrazilExportAvgMapper extends Mapper<LongWritable, Text, IntWritable, SumCountWritable> {

  private final IntWritable year = new IntWritable();

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] cols = CsvLineParser.split(value.toString());
    if (!CsvLineParser.isValidRow(cols) || CsvLineParser.isHeaderLine(cols)) {
      return;
    }
    if (!CsvLineParser.isBrazil(cols[CsvLineParser.COL_COUNTRY])) {
      return;
    }
    if (!CsvLineParser.isExport(cols[CsvLineParser.COL_FLOW])) {
      return;
    }
    Integer y = CsvLineParser.parseYear(cols[CsvLineParser.COL_YEAR]);
    if (y == null) {
      return;
    }
    Double price = CsvLineParser.parseDoubleField(cols[CsvLineParser.COL_PRICE]);
    if (price == null) {
      return;
    }
    year.set(y);
    context.write(year, new SumCountWritable(price, 1L));
  }
}
