import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

// Add these imports for JSON parsing and error handling
import scala.util.Try
import org.json4s._
import org.json4s.native.JsonMethods._

object LogProcessor {
  def main(args: Array[String]): Unit = {
    try {
      // Create Spark configuration
      val conf = new SparkConf()
        .setAppName("LogProcessor")
        .setMaster("local[*]")

      // Debug: Print all configuration settings
      conf.getAll.foreach(println)

      // Create Spark Streaming Context with 10-second batch interval
      val ssc = new StreamingContext(conf, Seconds(10))

      // Kafka configurations
      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "localhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "log-consumer-group",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )

      // Kafka topics to subscribe to
      val topics = Array("logs")

      // Create a DStream that represents streaming data from Kafka
      val stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
      )

      // Create a SparkSession
      val spark = SparkSession.builder()
        .appName("LogProcessor")
        .getOrCreate()

      // Define the MongoDB write configuration
      val writeConfig = WriteConfig(Map("uri" -> "mongodb://localhost:27017/logs.logsCollection"))

      // Process each RDD from the Kafka stream
      stream.foreachRDD { rdd =>
        // Convert each record of the RDD to a Row of log data
        val rowRDD = rdd.map(record => {
          val jsonValue = parse(record.value()) // Assuming you're using json4s for JSON parsing
          val pattern = "JString\\((.*?)\\)".r

          def extractWithoutJString(json: JValue): String = json match {
            case JString(value) => value
            case _ => "" // return empty string if not matched
          }

          val timestamp = extractWithoutJString(Try(jsonValue \ "timestamp").getOrElse(JNothing))
          val logLevel = extractWithoutJString(Try(jsonValue \ "logLevel").getOrElse(JNothing))
          val message = extractWithoutJString(Try(jsonValue \ "message").getOrElse(JNothing))

          Row(timestamp, logLevel, message)
        })
        // Define the schema for log data
        val logSchema = StructType(List(
          StructField("timestamp", StringType, true),
          StructField("logLevel", StringType, true),
          StructField("message", StringType, true)
        ))

        // Create a DataFrame from the RDD with the defined schema
        val logsDF = spark.createDataFrame(rowRDD, logSchema)

        // Save the DataFrame to MongoDB using the write configuration
        MongoSpark.save(logsDF, writeConfig)
      }

      // Start the Spark Streaming context
      ssc.start()

      // Wait for the computation to terminate
      ssc.awaitTermination()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        println(s"Error: ${e.getMessage}")
    }
  }
}
