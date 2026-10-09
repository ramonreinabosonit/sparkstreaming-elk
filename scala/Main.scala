import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, split}
import schemas.{BinanceSchema, CoinbaseSchema, KrakenSchema}

import scala.util.control.Exception

object Main {

  // Autoeliminar los evetos con 1 día de vida - Elastic
  def main(args: Array[String]): Unit = {
    println("Cargando librerías Spark")

    try {
//      val spark: SparkSession = SparkSession.builder().appName("SparkStreaming").master("local[*]").getOrCreate()
//      spark.sparkContext.setLogLevel("WARN")
//      return
    } catch {
      case e: Exception => println("Ha ocurrido un error cargando la SparkSession")
    }
    val spark: SparkSession = SparkSession.builder().appName("SparkStreaming").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // ================= RUTAS =================
    val binanceRutas = "C://Users//ramon.reina//IdeaProjects//SparkStreamingElk//src//main//resources//data//binance"
    val krakenRutas = "C://Users//ramon.reina//IdeaProjects//SparkStreamingElk//src//main//resources//data//kraken"
    val coinbaseRutas = "C://Users//ramon.reina//IdeaProjects//SparkStreamingElk//src//main//resources//data//coinbase"
    // =========================================

    // cambiar read por readStream, para usar spark structured streaming
    val binanceDF = spark.readStream.option("multiLine", true).schema(BinanceSchema.customSchema).json(binanceRutas)
    val krakenDF = spark.readStream.option("multiLine", true).schema(KrakenSchema.customSchema).json(krakenRutas)
    val coinbaseDF = spark.readStream.option("multiLine", true).schema(CoinbaseSchema.customSchema).json(coinbaseRutas)

    println("Limpiando el DataFrame")
    val cleanBinanceDF = binanceDF
      .withColumn("sede_ciudad", col("sede_ubication.ciudad"))
      .withColumn("sede_pais", col("sede_ubication.pais"))
      .withColumn("sede_latitud", col("sede_ubication.latitud"))
      .withColumn("sede_longitud", col("sede_ubication.longitud"))
      .withColumn("precio", col("precio").cast("double"))
      .withColumn("cantidad", col("cantidad").cast("double"))
      .withColumn("total", col("total").cast("double"))
      .withColumn("hora", split(col("fecha"), " ") (1))
      .withColumn("fecha", split(col("fecha"), " ") (0))

      .withColumn("transaccion_ciudad", col("ubicacion_transaccion.ciudad"))
      .withColumn("transaccion_pais", col("ubicacion_transaccion.pais"))
      .withColumn("transaccion_latitud", col("ubicacion_transaccion.latitud"))
      .withColumn("transaccion_longitud", col("ubicacion_transaccion.longitud"))
      .drop(col("sede_ubication")).drop(col("ubicacion_transaccion"))
      .withColumnRenamed("transactionId", "id_transaccion")
      .withColumnRenamed("operation_type", "tipo_operacion")
      .withColumnRenamed("pair", "par_activos")
//    cleanBinanceDF.show()

    val cleanKrakenDF = krakenDF
      .withColumn("sede_ciudad", col("fiscal_ubication.ciudad"))
      .withColumn("sede_pais", col("fiscal_ubication.pais"))
      .withColumn("sede_latitud", col("fiscal_ubication.latitud"))
      .withColumn("sede_longitud", col("fiscal_ubication.longitud"))
      .withColumn("precio", col("coin_value").cast("double"))
      .withColumn("cantidad", col("coin_operation_quantity").cast("double"))
      .withColumn("total", col("final_price").cast("double"))
      .withColumn("hora", split(col("date"), " ") (1))
      .withColumn("fecha", split(col("date"), " ") (0))

      .drop("coin_value").drop("coin_operation_quantity").drop("final_price").drop("date")

      .withColumn("transaccion_ciudad", col("user_ubication.ciudad"))
      .withColumn("transaccion_pais", col("user_ubication.pais"))
      .withColumn("transaccion_latitud", col("user_ubication.latitud"))
      .withColumn("transaccion_longitud", col("user_ubication.longitud"))
      .drop(col("fiscal_ubication")).drop(col("user_ubication"))
      .withColumnRenamed("transactionId", "id_transaccion")
      .withColumnRenamed("operation_type", "tipo_operacion")
      .withColumnRenamed("values", "par_activos")

    val cleanCoinbaseDF = coinbaseDF
      .withColumn("sede_ciudad", col("exchange_ubication.ciudad"))
      .withColumn("sede_pais", col("exchange_ubication.pais"))
      .withColumn("sede_latitud", col("exchange_ubication.latitud"))
      .withColumn("sede_longitud", col("exchange_ubication.longitud"))
      .withColumn("precio", col("price").cast("double"))
      .withColumn("cantidad", col("quantity").cast("double"))
      .withColumn("total", col("total_price").cast("double"))
      .withColumn("hora", split(col("date"), " ") (1))
      .withColumn("fecha", split(col("date"), " ") (0))

      .drop("price").drop("quantity").drop("total_price").drop("date")

      .withColumn("transaccion_ciudad", col("transaction_ubication.ciudad"))
      .withColumn("transaccion_pais", col("transaction_ubication.pais"))
      .withColumn("transaccion_latitud", col("transaction_ubication.latitud"))
      .withColumn("transaccion_longitud", col("transaction_ubication.longitud"))
      .drop(col("exchange_ubication")).drop(col("transaction_ubication"))
      .withColumnRenamed("transactionId", "id_transaccion")
      .withColumnRenamed("operation", "tipo_operacion")
      .withColumnRenamed("pair", "par_activos")

    val cleanMasterDF = cleanBinanceDF.unionByName(cleanKrakenDF).unionByName(cleanCoinbaseDF)

    println("Esperando 1 segundos")
    Thread.sleep(1000)

    val query = cleanMasterDF.writeStream.outputMode("append")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        println("\n======================================")
        println(s"BATCH: $batchId")
        println("======================================\n")

        val total = batchDF.count()
        println(s"Eventos recibidos en este batch: $total")

        println("\n----- DATOS DEL BATCH -----")

        batchDF.show(20, truncate = false)
//        batchDF.select("id_transaccion", "par_activos", "tipo_operacion", "precio", "cantidad", "total", "fecha",
//            "hora", "sede_ciudad", "transaccion_ciudad").show(20, truncate = false)

        println("======================================\n")

      }
      .option("checkpointLocation", "checkpoints/crypto_stream_test").start()
    query.awaitTermination()


    // NO SE USAN BUCLES???
//    while (true) {
//      cleanBinanceDF.show(100)
//      println("Esperando 5 segundos")
//      Thread.sleep(5000)
//      println("Ya")
//    }
//    cleanBinanceDF.show()


    //    def sendToElastic(df: DataFrame, indice: String, id: String = ""): Unit = {
//      println(s"Enviando $indice a Elastic")
//      var envioElastic = df.write.format("org.elasticsearch.spark.sql")
//        .option("es.resource", indice)
//        .option("es.nodes", "elasticsearch")
//        .option("es.port", "9200")
//        .option("es.nodes.wan.only", "true")
//        .option("es.net.http.auth.user", "elastic")
//        .option("es.net.http.auth.pass", "Password2026")
//        .mode("overwrite")
//
//      if(id.nonEmpty){
//        envioElastic = envioElastic.option("es.mapping.id", id)
//      }
//
//      envioElastic.save()
//
//      println(s"$indice enviado correctamente")
//    }
  }
}
