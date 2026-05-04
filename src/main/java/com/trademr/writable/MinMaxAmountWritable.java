package com.trademr.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MinMaxAmountWritable implements Writable {

  private double min = 0D;
  private double max = 0D;
  private boolean empty = true;

  public MinMaxAmountWritable() {}

  public MinMaxAmountWritable(double amount) {
    this.min = amount;
    this.max = amount;
    this.empty = false;
  }

  public double getMin() {
    return min;
  }

  public double getMax() {
    return max;
  }

  public boolean isEmpty() {
    return empty;
  }

  public void merge(MinMaxAmountWritable other) {
    if (other.empty) {
      return;
    }
    if (empty) {
      min = other.min;
      max = other.max;
      empty = false;
      return;
    }
    min = Math.min(min, other.min);
    max = Math.max(max, other.max);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(empty);
    out.writeDouble(min);
    out.writeDouble(max);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    empty = in.readBoolean();
    min = in.readDouble();
    max = in.readDouble();
  }
}
