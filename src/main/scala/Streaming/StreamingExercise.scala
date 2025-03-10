package Streaming

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object StreamingExercise {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("StreamingExercise")
      .getOrCreate()
    // Extraccion

    val schema = new StructType()
      .add("id_venta", StringType, true)
      .add("producto", StringType, true)
      .add("cantidad", StringType, true)
      .add("precio_unitario", StringType, true)
      .add("fecha", StringType, true)
      .add("cliente", StringType, true)

    val dataDF = spark.readStream
      .format("csv")
      .schema(schema)
      .option("header", "true")
      .option("delimiter", ";")
      .option("maxFilesPerTrigger", "1")
      .load(args(0))

    val dataWithTimeStampDF = dataDF.withColumn("timestamp", current_timestamp())

    // Transformacion

    val transformedDataDF = dataWithTimeStampDF.groupBy("cliente", "producto").count()

    // Load

    val queryConsole = transformedDataDF.writeStream
      .outputMode("complete")
      .format("console")
      .option("numRows", Int.MaxValue)
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()

    //    val queryDelta = transformedDataDF.writeStream
    //      .outputMode("complete")
    //      .format("delta")
    //      .option("checkpointLocation", "/tmp/checkpoint")
    //      .trigger(Trigger.ProcessingTime("30 seconds"))
    //      .start(args(1))

    // Await terminations

    queryConsole.awaitTermination()
    //    queryDelta.awaitTermination()

  }
}
