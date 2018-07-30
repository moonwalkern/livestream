package com.livestream.image

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ReadStream {


  case class Flight(_id: String, dofW: Integer,carrier: String, origin: String, dest:String ,crsdephour : Integer, crsdeptime:Double
                    ,depdelay:Double,crsarrtime:Double, arrdelay:Double ,crselapsedtime : Double, dist:Double) extends Serializable

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Read Kafka Feeds")
      .master("local")
      .getOrCreate() //This creates a new session if not existing

    println("Start of Image Streaming")


    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "MissingId",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val schema = StructType(Array(
      StructField("_id", StringType, true),
      StructField("dofW", IntegerType, true),
      StructField("carrier", StringType, true),
      StructField("origin", StringType, true),
      StructField("dest", StringType, true),
      StructField("crsdephour", IntegerType, true),
      StructField("crsdeptime", DoubleType, true),
      StructField("depdelay", DoubleType, true),
      StructField("crsarrtime", DoubleType, true),
      StructField("arrdelay", DoubleType, true),
      StructField("crselapsedtime", DoubleType, true),
      StructField("dist", DoubleType, true)
    ))

    val topics = Array("streamer")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val valueDStream: DStream[String] = stream.map(_.value())

    valueDStream.foreachRDD( rdd =>

      if(!rdd.isEmpty()){
        val spark = SparkSession.builder()
          .appName("ReadFile")
          .master("local")
          .config("spark.streaming.fileStream.minRememberDuration","1000")
          .getOrCreate() //This creates a new session if not existing
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        val df: Dataset[Flight] = spark.read.schema(schema).json(rdd).as[Flight]
        df.show()
        df.createOrReplaceGlobalTempView("flights")


      }

    )

    ssc.remember(Seconds(10))



    ssc.start()
    ssc.awaitTermination()


  }

}
