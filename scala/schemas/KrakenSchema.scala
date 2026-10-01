package schemas

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object KrakenSchema {

//  "id": 1,
//  "transactionId": "BN-A42EE473",
//  "fiscal_ubication": {
//    "ciudad": "Road Town",
//    "pais": "Islas Vírgenes Británicas",
//    "latitud": 18.4285,
//    "longitud": -64.6185
//  },
//  "values": "DOT/USDC",
//  "operation_type": "COMPRA",
//  "coin_value": "3.01",
//  "coin_operation_quantity": "638.511200",
//  "final_price": "1921.92",
//  "date": "30/09/2026 10:36:15",
//  "user_ubication": {
//    "ciudad": "London",
//    "pais": "Reino Unido",
//    "latitud": 51.5074,
//    "longitud": -0.1278,
//  }

  val customSchema: StructType = StructType(Seq(
    StructField("id", IntegerType, nullable = false),
    StructField("transactionId", StringType, nullable = false),
    StructField("fiscal_ubication", StructType(Seq(
      StructField("ciudad", StringType, nullable = false),
      StructField("pais", StringType, nullable = false),
      StructField("latitud", DoubleType, nullable = false),
      StructField("longitud", DoubleType, nullable = false)
    ))),
    StructField("values", StringType, nullable = false),
    StructField("operation_type", StringType, nullable = false),
    StructField("coin_value", StringType, nullable = false),
    StructField("coin_operation_quantity", StringType, nullable = false),
    StructField("final_price", StringType, nullable = false),
    StructField("date", StringType, nullable = false),
    StructField("user_ubication", StructType(Seq(
      StructField("ciudad", StringType, nullable = false),
      StructField("pais", StringType, nullable = false),
      StructField("latitud", DoubleType, nullable = false),
      StructField("longitud", DoubleType, nullable = false)
    )))
  ))
}
