package it.unitn.dbtrento.spark.utils;

public enum FieldDelimiter {
  COMMA(','), TAB('\t'), SPACE(' '), COLON(':'), SEMICOLON(';');

  FieldDelimiter(char fieldDelimiter){
    this.fieldDelimiter = fieldDelimiter;
  }

  private final char fieldDelimiter;

  public char getFieldDelimiter() {
    return fieldDelimiter;
  }
}
