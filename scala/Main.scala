import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, split}
import schemas.{BinanceSchema, CoinbaseSchema, KrakenSchema}

object Main {

  // Autoeliminar los evetos con 1 día de vida - Elastic
  def main(args: Array[String]): Unit = {
    println("Probando Spark")

    val spark: SparkSession = SparkSession.builder().appName("SparkStreaming").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // DataFrame en Crudo, ahora tocara procesarlo y normalizarlo
    val binanceDF = spark.read.option("multiLine", "true").schema(BinanceSchema.customSchema).json("C://Users//ramon.reina//IdeaProjects//SparkStreamingElk//src//main//resources//data//prueba.json")
    binanceDF.show()

    val krakenDF = spark.read.option("multiLine", "true").schema(KrakenSchema.customSchema).json("C://Users//ramon.reina//IdeaProjects//SparkStreamingElk//src//main//resources//data//kraken.json")
    krakenDF.show()

    val coinbaseDF = spark.read.option("multiLine", "true").schema(CoinbaseSchema.customSchema).json("C://Users//ramon.reina//IdeaProjects//SparkStreamingElk//src//main//resources//data//coinbase.json")
    coinbaseDF.show()

//    krakenDF.printSchema()

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

    println("Esperando 5 segundos")
    Thread.sleep(5000)

    while (true) {
      cleanBinanceDF.show(100)
      println("Esperando 5 segundos")
      Thread.sleep(5000)
      println("Ya")
    }
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
