package me.rotemfo.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, SparkSession}
import scopt.OptionParser

import java.sql.Date
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import scala.util.{Failure, Try}

trait SparkApp[P <: Product] extends Logging {
  //@formatter:off
  protected def process(p: P, sparkSession: SparkSession): Unit
  protected def getParser: OptionParser[P]
  protected def configSparkSession(p: P, sessionBuilder: SparkSession.Builder): Unit = {}
  //@formatter:on
}

abstract class BaseApplication[P <: Product](p: P) extends SparkApp[P] {
  def main(args: Array[String]): Unit = {
    getParser.parse(args, p).foreach { p =>
      val sparkSessionBuilder = SparkSession.builder()
        .enableHiveSupport()
        .config("spark.hadoop.hive.exec.dynamic.partition", "true")
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.sql.broadcastTimeout", 6600)

      configSparkSession(p, sparkSessionBuilder)

      val sparkSession = sparkSessionBuilder.getOrCreate
      val hadoopConfiguration = sparkSession.sparkContext.hadoopConfiguration
      hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "")
      hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "")

      val processResults: Try[Unit] = Try(process(p, sparkSession))
      sparkSession.sparkContext.getPersistentRDDs.values.foreach(_.unpersist())
      sparkSession.close()
      processResults match {
        case Failure(ex) => throw ex
        case _ =>
      }
    }
  }
}

object BaseApplication {
  val dateHourPattern = "yyyy-MM-dd-HH"
  val dateHourPatternFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(dateHourPattern)
  val datePattern = "yyyy-MM-dd"
  val datePatternFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(datePattern)

  def filterDataFrameWithDateHourPartitions(dateHourRange: Seq[LocalDateTime], dateColumn: Column = col("date_"), hourColumn: Column = col("hour")): Column =
    dateHourRange
      .map(ldt => dateColumn.equalTo(Date.valueOf(ldt.toLocalDate)) and hourColumn.equalTo(ldt.getHour))
      .reduce(_ or _)

  def filterDataFrameWithDatePartitions(dateHourRange: Seq[LocalDate], dateColumn: String = "date_"): Column =
    dateHourRange
      .map(ldt => col(dateColumn).equalTo(Date.valueOf(ldt)))
      .reduce(_ or _)
}