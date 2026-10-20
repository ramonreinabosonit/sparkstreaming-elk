import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import schemas.{BinanceSchema, CoinbaseSchema, KrakenSchema}

object MainTest {

  def main(args: Array[String]): Unit = {

    println("Cargando librerías Spark")
    val spark: SparkSession = SparkSession.builder().appName("SparkStreamingTest").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // ================= RUTAS =================
    val binanceRutas = "C://Users//ramon.reina//IdeaProjects//SparkStreamingElk//src//main//resources//data//binance"
    val krakenRutas = "C://Users//ramon.reina//IdeaProjects//SparkStreamingElk//src//main//resources//data//kraken"
    val coinbaseRutas = "C://Users//ramon.reina//IdeaProjects//SparkStreamingElk//src//main//resources//data//coinbase"
    // =========================================

    val binanceDF = spark.readStream.option("multiLine", true).schema(BinanceSchema.customSchema).json(binanceRutas)
    val krakenDF = spark.readStream.option("multiLine", true).schema(KrakenSchema.customSchema).json(krakenRutas)
    val coinbaseDF = spark.readStream.option("multiLine", true).schema(CoinbaseSchema.customSchema).json(coinbaseRutas)

    println("Normalizando los datos...")

    val cleanBinanceDF = binanceDF
      .withColumn("sede_ciudad", col("sede_ubication.ciudad"))
      .withColumn("sede_pais", col("sede_ubication.pais"))
      .withColumn("sede_latitud", col("sede_ubication.latitud"))
      .withColumn("sede_longitud", col("sede_ubication.longitud"))
      .withColumn("hora", split(col("fecha"), " ")(1)).withColumn("fecha", split(col("fecha"), " ")(0))
      .withColumn("transaccion_ciudad", col("ubicacion_transaccion.ciudad")).withColumn("transaccion_pais", col("ubicacion_transaccion.pais"))
      .withColumn("transaccion_latitud", col("ubicacion_transaccion.latitud")).withColumn("transaccion_longitud", col("ubicacion_transaccion.longitud"))

      .drop("sede_ubication", "ubicacion_transaccion")

      .withColumnRenamed("transactionId", "id_transaccion").withColumnRenamed("operation_type", "tipo_operacion")
      .withColumnRenamed("pair", "par_activos").withColumn("fuente", lit("binance"))

    val cleanKrakenDF = krakenDF.withColumn("sede_ciudad", col("fiscal_ubication.ciudad"))
      .withColumn("sede_pais", col("fiscal_ubication.pais")).withColumn("sede_latitud", col("fiscal_ubication.latitud"))
      .withColumn("sede_longitud", col("fiscal_ubication.longitud")).withColumn("precio", col("coin_value"))
      .withColumn("cantidad", col("coin_operation_quantity")).withColumn("total", col("final_price"))
      .withColumn("hora", split(col("date"), " ")(1)).withColumn("fecha", split(col("date"), " ")(0))

      .drop("coin_value", "coin_operation_quantity", "final_price", "date")

      .withColumn("transaccion_ciudad", col("user_ubication.ciudad")).withColumn("transaccion_pais", col("user_ubication.pais"))
      .withColumn("transaccion_latitud", col("user_ubication.latitud")).withColumn("transaccion_longitud", col("user_ubication.longitud"))

      .drop("fiscal_ubication", "user_ubication")

      .withColumnRenamed("transactionId", "id_transaccion").withColumnRenamed("operation_type", "tipo_operacion")
      .withColumnRenamed("values", "par_activos").withColumn("fuente", lit("kraken"))

    val cleanCoinbaseDF = coinbaseDF
      .withColumn("sede_ciudad", col("exchange_ubication.ciudad"))
      .withColumn("sede_pais", col("exchange_ubication.pais"))
      .withColumn("sede_latitud", col("exchange_ubication.latitud"))
      .withColumn("sede_longitud", col("exchange_ubication.longitud"))
      .withColumn("precio", col("price"))
      .withColumn("cantidad", col("quantity"))
      .withColumn("total", col("total_price"))
      .withColumn("hora", split(col("date"), " ")(1))
      .withColumn("fecha", split(col("date"), " ")(0))

      .drop("price", "quantity", "total_price", "date")

      .withColumn("transaccion_ciudad", col("transaction_ubication.ciudad")).withColumn("transaccion_pais", col("transaction_ubication.pais"))
      .withColumn("transaccion_latitud", col("transaction_ubication.latitud")).withColumn("transaccion_longitud", col("transaction_ubication.longitud"))

      .drop("exchange_ubication", "transaction_ubication")

      .withColumnRenamed("transactionId", "id_transaccion")
      .withColumnRenamed("operation", "tipo_operacion")
      .withColumnRenamed("pair", "par_activos")
      .withColumn("fuente", lit("coinbase"))

    val cleanMasterDF = cleanBinanceDF.unionByName(cleanKrakenDF).unionByName(cleanCoinbaseDF)

    val datosValidados = cleanMasterDF
      .withColumn("fecha_validada", to_timestamp(concat_ws(" ", col("fecha"), col("hora")), "dd/MM/yyyy HH:mm:ss"))
      .withColumn("precio_num", col("precio").cast("double"))
      .withColumn("cantidad_num", col("cantidad").cast("double"))
      .withColumn("total_num", col("total").cast("double"))
      .withColumn("lat_num", col("transaccion_latitud").cast("double"))
      .withColumn("lon_num", col("transaccion_longitud").cast("double"))
      .withColumn("sede_lat_num", col("sede_latitud").cast("double"))
      .withColumn("sede_lon_num", col("sede_longitud").cast("double"))
      .withColumn("error",
        when(col("id_transaccion").isNull || trim(col("id_transaccion")) === "", lit("ID ausente"))
          .when(col("fecha_validada").isNull, lit("Fecha inválida"))
          .when(col("precio_num").isNull || col("precio_num") <= 0, lit("Precio inválido"))
          .when(col("cantidad_num").isNull || col("cantidad_num") <= 0, lit("Cantidad inválida"))
          .when(col("total_num").isNull || col("total_num") <= 0, lit("Total inválido"))
          .when(col("lat_num").isNull || col("lon_num").isNull || col("lat_num") < -90 || col("lat_num") > 90 || col("lon_num") < -180 || col("lon_num") > 180, lit("Coordenadas inválidas"))
          .when(col("sede_lat_num").isNull || col("sede_lon_num").isNull || col("sede_lat_num") < -90 || col("sede_lat_num") > 90 || col("sede_lon_num") < -180 || col("sede_lon_num") > 180, lit("Coordenadas de sede inválidas"))
      )

    try {
      println("Iniciando Spark Structured Streaming...")

      var ultimaActividad = System.currentTimeMillis()
      val query = datosValidados.writeStream.queryName("MainTestTerminal")
        .outputMode("append").option("checkpointLocation", "checkpoints/crypto_stream_test")
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          if (!batchDF.isEmpty) {
            val total = batchDF.count()
            val validos = batchDF.filter(col("error").isNull)
            val errores = batchDF.filter(col("error").isNotNull)
            val totalValidos = validos.count()
            val totalErrores = errores.count()
            ultimaActividad = System.currentTimeMillis()
            println("\n==================================================")
            println(s"              BATCH: $batchId")
            println("==================================================")
            println(s"Transacciones recibidas : $total")
            println(s"Transacciones válidas   : $totalValidos")
            println(s"Transacciones con error : $totalErrores")
            println("==================================================")
            if (totalValidos > 0) {
              println("\n----------- TRANSACCIONES VÁLIDAS -----------")
              validos.select(
                col("id_transaccion"), col("fuente"), col("par_activos"), col("tipo_operacion"),
                col("precio_num").as("precio"), col("cantidad_num").as("cantidad"), col("total_num").as("total"),
                col("fecha"), col("hora"), col("sede_ciudad"), col("sede_pais"),
                col("transaccion_ciudad"), col("transaccion_pais"), col("transaccion_latitud"), col("transaccion_longitud")
              ).show(50, truncate = false)
            }
            if (totalErrores > 0) {
              println("\n----------- TRANSACCIONES CON ERRORES -----------")
              errores.select(
                col("id_transaccion"), col("fuente"), col("par_activos"), col("tipo_operacion"),
                col("precio"), col("cantidad"), col("total"), col("fecha"), col("hora"),
                col("sede_ciudad"), col("transaccion_ciudad"), col("transaccion_latitud"),
                col("transaccion_longitud"), col("error")
              ).show(50, truncate = false)
            }
            println(s"Fin del batch $batchId")
            println("==================================================")
          }
        }.start()

      while (query.isActive) {
        Thread.sleep(5000)
        if (System.currentTimeMillis() - ultimaActividad >= 60000) {
          println("60 segundos sin actividad. Deteniendo el streaming...")
          query.stop()
        }
      }

      query.awaitTermination()

    } catch {
      case e: Exception =>
        println(s"Error durante el streaming de prueba: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
      println("Spark finalizado.")
    }

  }
}
