package it.unitn.dbtrento.spark.utils;

public enum InputFormat {
  CSV, PARQUET, TSV, SSV;

  public char getFieldDelimiter() {
    switch (this) {
      case CSV:
        return FieldDelimiter.COMMA.getFieldDelimiter();
      case PARQUET:
        return FieldDelimiter.COMMA.getFieldDelimiter();
      case TSV:
        return FieldDelimiter.TAB.getFieldDelimiter();
      case SSV:
        return FieldDelimiter.SEMICOLON.getFieldDelimiter();
      default:
        System.err.println("Error while getting the field delimiter.");
        return ',';
    }
  }
}
