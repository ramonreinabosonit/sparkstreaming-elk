import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.apache.spark.sql.functions.{col, split, lit, struct, to_timestamp, concat_ws}
import org.apache.spark.sql.functions._
import schemas.{BinanceSchema, CoinbaseSchema, KrakenSchema}
import scala.util.control.Exception

object Main {

  // Autoeliminar los evetos con 1 día de vida - Elastic
  def main(args: Array[String]): Unit = {
    println("Cargando librerías Spark")

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
    // tener cuidado con los cast
    // convierten en null las incidencias recibidas
    val cleanBinanceDF = binanceDF
      .withColumn("sede_ciudad", col("sede_ubication.ciudad"))
      .withColumn("sede_pais", col("sede_ubication.pais"))
      .withColumn("sede_latitud", col("sede_ubication.latitud"))
      .withColumn("sede_longitud", col("sede_ubication.longitud"))
//      .withColumn("precio", col("precio").cast("double"))
//      .withColumn("cantidad", col("cantidad").cast("double"))
//      .withColumn("total", col("total").cast("double"))
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

      // lit() sirve para crear una columna con un valor constante
      .withColumn("fuente", lit("binance"))
//    if (col("sede_latitud") > 90.0 || col("sede_latitud") < -90.0)

    val cleanKrakenDF = krakenDF
      .withColumn("sede_ciudad", col("fiscal_ubication.ciudad"))
      .withColumn("sede_pais", col("fiscal_ubication.pais"))
      .withColumn("sede_latitud", col("fiscal_ubication.latitud"))
      .withColumn("sede_longitud", col("fiscal_ubication.longitud"))
      .withColumn("precio", col("coin_value"))
      .withColumn("cantidad", col("coin_operation_quantity"))
      .withColumn("total", col("final_price"))
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

      .withColumn("fuente", lit("kraken"))

    val cleanCoinbaseDF = coinbaseDF
      .withColumn("sede_ciudad", col("exchange_ubication.ciudad"))
      .withColumn("sede_pais", col("exchange_ubication.pais"))
      .withColumn("sede_latitud", col("exchange_ubication.latitud"))
      .withColumn("sede_longitud", col("exchange_ubication.longitud"))
      .withColumn("precio", col("price"))
      .withColumn("cantidad", col("quantity"))
      .withColumn("total", col("total_price"))
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

      .withColumn("fuente", lit("coinbase"))

    val cleanMasterDF = cleanBinanceDF.unionByName(cleanKrakenDF).unionByName(cleanCoinbaseDF)

    // VALIDACION, ANALIZAR SI EXISTEN O NO INCIDENCIAS - PREPARACION ELASTIC
    val datosValidados = cleanMasterDF
      .withColumn("fecha_validada", to_timestamp(concat_ws(" ", col("fecha"), col("hora")), "dd/MM/yyyy HH:mm:ss"))
      .withColumn("precio_num", col("precio").cast("double"))
      .withColumn("cantidad_num", col("cantidad").cast("double"))
      .withColumn("total_num", col("total").cast("double"))
      .withColumn("lat_num", col("transaccion_latitud").cast("double"))
      .withColumn("lon_num", col("transaccion_longitud").cast("double"))
      .withColumn("sede_lat_num", col("sede_latitud").cast("double"))
      .withColumn("sede_lon_num", col("sede_longitud").cast("double"))

      // limitacion de la columna error - recoge unicamente la primera incidencia
      .withColumn("error",
        when(col("id_transaccion").isNull || trim(col("id_transaccion")) === "", lit("ID ausente"))
          .when(col("fecha_validada").isNull, lit("Fecha inválida"))
          .when(col("precio_num").isNull || col("precio_num") <= 0, lit("Precio inválido"))
          .when(col("cantidad_num").isNull || col("cantidad_num") <= 0, lit("Cantidad inválida"))
          .when(col("total_num").isNull || col("total_num") <= 0, lit("Total inválido"))
          .when(col("lat_num").isNull || col("lon_num").isNull ||
            col("lat_num") < -90 || col("lat_num") > 90 || col("lon_num") < -180 || col("lon_num") > 180,
            lit("Coordenadas inválidas"))
          .when(
            col("sede_lat_num").isNull || col("sede_lon_num").isNull ||
              col("sede_lat_num") < -90 || col("sede_lat_num") > 90 ||
              col("sede_lon_num") < -180 || col("sede_lon_num") > 180,
            lit("Coordenadas de sede inválidas")
          ))

    try {
      println("Empezando el streaming")
      var ultimaActividad = System.currentTimeMillis()
      val query = datosValidados.writeStream
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          if (!batchDF.isEmpty) {

            val validos = batchDF
              .filter(col("error").isNull)
              .withColumnRenamed("fecha_validada", "@timestamp")
              .withColumn("precio", col("precio_num"))
              .withColumn("cantidad", col("cantidad_num"))
              .withColumn("total", col("total_num"))
              .withColumn("event", struct(col("id_transaccion").as("id"),
                lit("crypto_transaction").as("type"), col("fuente").as("source")))
              .withColumn("location", struct(struct(
                col("lat_num").as("lat"), col("lon_num").as("lon")).as("point"),
                col("transaccion_ciudad").as("city"), col("transaccion_pais").as("country")))
              .withColumn("exchange", struct(struct(
                col("sede_lat_num").as("lat"), col("sede_lon_num").as("lon")).as("point")))
              .drop("precio_num", "cantidad_num", "total_num", "lat_num", "lon_num", "sede_lat_num", "sede_lon_num", "error")

            val errores = batchDF
              .filter(col("error").isNotNull)
              .withColumn("@timestamp", coalesce(col("fecha_validada"), current_timestamp()))
              .withColumn("status", lit("ERROR"))
              .withColumn("event", struct(
                col("id_transaccion").as("id"), lit("crypto_transaction").as("type"), col("fuente").as("source")))
              .withColumn("precio_original", col("precio").cast("string"))
              .withColumn("cantidad_original", col("cantidad").cast("string"))
              .withColumn("total_original", col("total").cast("string"))
              .withColumn("precio", col("precio_num"))
              .withColumn("cantidad", col("cantidad_num"))
              .withColumn("total", col("total_num"))
              .withColumn("fecha_original", col("fecha").cast("string"))
              .withColumn("hora_original", col("hora").cast("string"))
              .withColumn("fecha", when(col("fecha_validada").isNotNull,
                  date_format(col("fecha_validada"), "dd/MM/yyyy")).otherwise(lit(null).cast("string")))
              .withColumn("hora", when(col("fecha_validada").isNotNull,
                  date_format(col("fecha_validada"), "HH:mm:ss")).otherwise(lit(null).cast("string")))
              .drop("fecha_validada", "precio_num", "cantidad_num", "total_num", "lat_num", "lon_num", "sede_lat_num", "sede_lon_num")

            enviarAElastic(validos, "crypto-streaming")
            enviarAElastic(errores, "crypto-streaming-errors")
            ultimaActividad = System.currentTimeMillis()
          }
        }
        .option("checkpointLocation", "checkpoints/crypto-streaming")
        .start()

      while (query.isActive) {
        //println("Empezando cuenta atras inactividad")
        Thread.sleep(5000)
        if (System.currentTimeMillis() - ultimaActividad >= 60000) {
          println("Minuto de inactividad alcanzado, apagando el streaming...")
          query.stop()
        }
      }
    } catch {
      case e: Exception =>
        println(s"Ha habido un problema subiendo a Elastic...\n${e.getMessage}")
    }

    def enviarAElastic(df: DataFrame, indice: String): Unit = {
      if (!df.isEmpty) {
        df.write
          .format("org.elasticsearch.spark.sql")
          .option("es.nodes", "localhost")
          .option("es.port", "9200")
          .option("es.nodes.wan.only", "true")
          .option("es.resource", indice)
          .option("es.net.http.auth.user", "elastic")
          .option("es.net.http.auth.pass", "Password2026")
          .mode("append")
          .save()
      }
    }
  }
}
