package com.trademr.common;

import java.util.Locale;

public final class CsvLineParser {

  public static final int COL_COUNTRY = 0;
  public static final int COL_YEAR = 1;
  public static final int COL_FLOW = 4;
  public static final int COL_PRICE = 5;
  public static final int COL_AMOUNT = 8;
  public static final int COL_CATEGORY = 9;
  public static final int EXPECTED_COLUMNS = 10;

  private CsvLineParser() {}

  public static String stripBom(String line) {
    if (line != null && !line.isEmpty() && line.charAt(0) == '\uFEFF') {
      return line.substring(1);
    }
    return line;
  }

  public static String[] split(String line) {
    if (line == null) {
      return new String[0];
    }
    return stripBom(line).split(";", -1);
  }

  public static boolean isHeaderLine(String[] cols) {
    if (cols.length < 2) {
      return true;
    }
    String c0 = trim(cols[COL_COUNTRY]);
    String c1 = trim(cols[COL_YEAR]);
    if ("country".equalsIgnoreCase(c0)) {
      return true;
    }
    return !isInt(c1);
  }

  public static boolean isValidRow(String[] cols) {
    return cols.length == EXPECTED_COLUMNS;
  }

  public static boolean isExport(String flow) {
    if (flow == null) {
      return false;
    }
    return "export".equalsIgnoreCase(trim(flow));
  }

  public static boolean isBrazil(String country) {
    if (country == null) {
      return false;
    }
    String n = trim(country).toLowerCase(Locale.ROOT);
    return "brazil".equals(n) || "brasil".equals(n);
  }

  public static Integer parseYear(String raw) {
    if (raw == null || raw.isBlank()) {
      return null;
    }
    try {
      return Integer.parseInt(trim(raw));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  public static Double parseDoubleField(String raw) {
    if (raw == null || raw.isBlank()) {
      return null;
    }
    String n = trim(raw).replace(',', '.');
    try {
      return Double.parseDouble(n);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  public static String trim(String s) {
    return s == null ? "" : s.trim();
  }

  private static boolean isInt(String s) {
    if (s == null || s.isBlank()) {
      return false;
    }
    try {
      Integer.parseInt(trim(s));
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }
}
