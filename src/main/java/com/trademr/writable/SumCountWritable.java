package com.trademr.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SumCountWritable implements Writable {

  private double sum = 0D;
  private long count = 0L;

  public SumCountWritable() {}

  public SumCountWritable(double sum, long count) {
    this.sum = sum;
    this.count = count;
  }

  public double getSum() {
    return sum;
  }

  public long getCount() {
    return count;
  }

  public void add(double value) {
    sum += value;
    count += 1;
  }

  public void merge(SumCountWritable other) {
    this.sum += other.sum;
    this.count += other.count;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeDouble(sum);
    out.writeLong(count);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    sum = in.readDouble();
    count = in.readLong();
  }
}
