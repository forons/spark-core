package it.unitn.dbtrento.spark.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;

public class UDFRegisterer {

  private static List<UDFFunction> udfList = new ArrayList<>();

  public static void addNewUDF(UDFFunction udf) {
    udfList.add(udf);
  }

  public static void addNewUDF(String name, UDF1<?, ?> udf, DataType dataType) {
    udfList.add(new UDFFunction(name, udf, dataType));
  }

  public static void registerUDFs(SparkSession spark) throws Exception {
    for (UDFFunction udf : udfList) {
      if (udf.getUDF1() != null) {
        spark.udf().register(udf.getName(), udf.getUDF1(), udf.getDataType());
      } else if (udf.getUDF2() != null) {
        spark.udf().register(udf.getName(), udf.getUDF2(), udf.getDataType());
      } else if (udf.getUDF3() != null) {
        spark.udf().register(udf.getName(), udf.getUDF3(), udf.getDataType());
      } else if (udf.getUDF4() != null) {
        spark.udf().register(udf.getName(), udf.getUDF4(), udf.getDataType());
      } else if (udf.getUDF5() != null) {
        spark.udf().register(udf.getName(), udf.getUDF5(), udf.getDataType());
      } else if (udf.getUDF6() != null) {
        spark.udf().register(udf.getName(), udf.getUDF6(), udf.getDataType());
      } else if (udf.getUDF7() != null) {
        spark.udf().register(udf.getName(), udf.getUDF7(), udf.getDataType());
      } else if (udf.getUDF8() != null) {
        spark.udf().register(udf.getName(), udf.getUDF8(), udf.getDataType());
      } else if (udf.getUDF9() != null) {
        spark.udf().register(udf.getName(), udf.getUDF9(), udf.getDataType());
      } else if (udf.getUDF10() != null) {
        spark.udf().register(udf.getName(), udf.getUDF10(), udf.getDataType());
      } else if (udf.getUDF11() != null) {
        spark.udf().register(udf.getName(), udf.getUDF11(), udf.getDataType());
      } else if (udf.getUDF12() != null) {
        spark.udf().register(udf.getName(), udf.getUDF12(), udf.getDataType());
      } else if (udf.getUDF13() != null) {
        spark.udf().register(udf.getName(), udf.getUDF13(), udf.getDataType());
      } else if (udf.getUDF14() != null) {
        spark.udf().register(udf.getName(), udf.getUDF14(), udf.getDataType());
      } else if (udf.getUDF15() != null) {
        spark.udf().register(udf.getName(), udf.getUDF15(), udf.getDataType());
      } else if (udf.getUDF16() != null) {
        spark.udf().register(udf.getName(), udf.getUDF16(), udf.getDataType());
      } else if (udf.getUDF17() != null) {
        spark.udf().register(udf.getName(), udf.getUDF17(), udf.getDataType());
      } else if (udf.getUDF18() != null) {
        spark.udf().register(udf.getName(), udf.getUDF18(), udf.getDataType());
      } else if (udf.getUDF19() != null) {
        spark.udf().register(udf.getName(), udf.getUDF19(), udf.getDataType());
      } else if (udf.getUDF20() != null) {
        spark.udf().register(udf.getName(), udf.getUDF20(), udf.getDataType());
      } else if (udf.getUDF21() != null) {
        spark.udf().register(udf.getName(), udf.getUDF21(), udf.getDataType());
      } else if (udf.getUDF22() != null) {
        spark.udf().register(udf.getName(), udf.getUDF22(), udf.getDataType());
      } else {
        throw new Exception("UDF function not recognized!");
      }
    }
  }
}
