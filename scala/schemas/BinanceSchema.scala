package schemas

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}


object BinanceSchema {

  val customSchema: StructType = StructType(Seq(
    //StructField("id", IntegerType, nullable = true),
    StructField("transactionId", StringType, nullable = true),
    StructField("sede_ubication", StructType(Seq(
      StructField("ciudad", StringType, nullable = true),
      StructField("pais", StringType, nullable = true),
      StructField("latitud", DoubleType, nullable = true),
      StructField("longitud", DoubleType, nullable = true)
    )), nullable = true),
    StructField("pair", StringType, nullable = true),
    StructField("operation_type", StringType, nullable = true),
    StructField("precio", StringType, nullable = true),
    StructField("cantidad", StringType, nullable = true),
    StructField("total", StringType, nullable = true),
    StructField("fecha", StringType, nullable = true),
    StructField("ubicacion_transaccion", StructType(Seq(
      StructField("ciudad", StringType, nullable = true),
      StructField("pais", StringType, nullable = true),
      StructField("latitud", DoubleType, nullable = true),
      StructField("longitud", DoubleType, nullable = true),
    )), nullable = true)
  ))
}
