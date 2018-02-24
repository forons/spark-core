package eu.unitn.disi.db.spark.io;

public enum Format {
  CSV(','), SSV(';'), TSV('\t'), JSON(Character.MAX_VALUE), SINGLE_CSV(','), PARQUET(
      Character.MAX_VALUE);

  private char fieldDelimiter;

  Format(char fieldDelimiter) {
    this.fieldDelimiter = fieldDelimiter;
  }

  public char getFieldDelimiter() {
    return fieldDelimiter;
  }
}
