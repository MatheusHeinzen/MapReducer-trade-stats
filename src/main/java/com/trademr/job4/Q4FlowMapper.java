package com.trademr.job4;

import com.trademr.common.CsvLineParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Q4FlowMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

  private final Text flow = new Text();
  private final LongWritable one = new LongWritable(1);

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] cols = CsvLineParser.split(value.toString());
    if (!CsvLineParser.isValidRow(cols) || CsvLineParser.isHeaderLine(cols)) {
      return;
    }
    String f = CsvLineParser.trim(cols[CsvLineParser.COL_FLOW]);
    if (f.isEmpty()) {
      return;
    }
    flow.set(f);
    context.write(flow, one);
  }
}
