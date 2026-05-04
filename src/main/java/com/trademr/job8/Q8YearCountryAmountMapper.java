package com.trademr.job8;

import com.trademr.common.CsvLineParser;
import com.trademr.writable.MinMaxAmountWritable;
import com.trademr.writable.YearCountryKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Q8YearCountryAmountMapper extends Mapper<LongWritable, Text, YearCountryKey, MinMaxAmountWritable> {

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
    String country = CsvLineParser.trim(cols[CsvLineParser.COL_COUNTRY]);
    if (country.isEmpty()) {
      return;
    }
    Double amount = CsvLineParser.parseDoubleField(cols[CsvLineParser.COL_AMOUNT]);
    if (amount == null) {
      return;
    }
    context.write(new YearCountryKey(y, country), new MinMaxAmountWritable(amount));
  }
}
