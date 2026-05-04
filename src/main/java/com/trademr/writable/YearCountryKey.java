package com.trademr.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class YearCountryKey implements WritableComparable<YearCountryKey> {

  private int year = 0;
  private final Text country = new Text();

  public YearCountryKey() {}

  public YearCountryKey(int year, Text country) {
    this.year = year;
    this.country.set(textOrEmpty(country));
  }

  public YearCountryKey(int year, String country) {
    this.year = year;
    this.country.set(country == null ? "" : country);
  }

  public int getYear() {
    return year;
  }

  public void setYear(int year) {
    this.year = year;
  }

  public Text getCountry() {
    return country;
  }

  public void setCountry(Text c) {
    country.set(textOrEmpty(c));
  }

  public void setCountry(String c) {
    country.set(c == null ? "" : c);
  }

  private static String textOrEmpty(Text t) {
    return t == null ? "" : t.toString();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(year);
    country.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    year = in.readInt();
    country.readFields(in);
  }

  @Override
  public int compareTo(YearCountryKey o) {
    int y = Integer.compare(year, o.year);
    if (y != 0) {
      return y;
    }
    return country.compareTo(o.country);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof YearCountryKey)) {
      return false;
    }
    YearCountryKey other = (YearCountryKey) obj;
    return year == other.year && country.equals(other.country);
  }

  @Override
  public int hashCode() {
    return Objects.hash(year, country.toString());
  }
}
