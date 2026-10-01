package schemas

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object CoinbaseSchema {

//  "id": 1,
//  "transactionId": "CB-DA991224",
//  "exchange_ubication": {
//    "ciudad": "New York",
//    "pais": "Estados Unidos",
//    "latitud": 40.7142,
//    "longitud": -74.0059
//  },
//  "pair": "SOL/USDC",
//  "operation": "VENTA",
//  "price": "146.09",
//  "quantity": "44.000000",
//  "total_price": "6427.96",
//  "date": "30/09/2026 09:36:24",
//  "transaction_ubication": {
//    "ciudad": "New York",
//    "pais": "Estados Unidos",
//    "latitud": 40.7128,
//    "longitud": -74.006
//  }

  val customSchema: StructType = StructType(Seq(
    StructField("id", IntegerType, nullable = false),
    StructField("transactionId", StringType, nullable = false),
    StructField("exchange_ubication", StructType(Seq(
      StructField("ciudad", StringType, nullable = false),
      StructField("pais", StringType, nullable = false),
      StructField("latitud", DoubleType, nullable = false),
      StructField("longitud", DoubleType, nullable = false),
    ))),
    StructField("pair", StringType, nullable = false),
    StructField("operation", StringType, nullable = false),
    StructField("price", StringType, nullable = false),
    StructField("quantity", StringType, nullable = false),
    StructField("total_price", StringType, nullable = false),
    StructField("date", StringType, nullable = false),
    StructField("transaction_ubication", StructType(Seq(
      StructField("ciudad", StringType, nullable = false),
      StructField("pais", StringType, nullable = false),
      StructField("latitud", DoubleType, nullable = false),
      StructField("longitud", DoubleType, nullable = false),
    )))

  ))
}
