package me.rotemfo.spark

import me.rotemfo.spark.BaseApplication.{datePatternFormatter, filterDataFrameWithDatePartitions}
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.{SaveMode, SparkSession}
import scopt.OptionParser

import java.time.LocalDate

final case class CounterParams(dateInputs: Seq[LocalDate] = Seq.empty[LocalDate],
                               dbName: Option[String] = None,
                               tableName: Option[String] = None,
                               selection: Option[String] = None,
                               grouping: Seq[String] = Seq("category"),
                               outputPath: Option[String] = None
                              )

// @formatter:off
object CounterParser extends OptionParser[CounterParams](programName = "Counter Application") {
  opt[Seq[String]]("date-inputs")                .required.action { (x, p) => p.copy(dateInputs = x.map(LocalDate.parse(_, datePatternFormatter))) }
    .validate(x => if (x.length == 1) success else if (x.head <= x.tail.head) success else failure("start date is after end date"))
  opt[String]("db-name")                         .required.action { (x, p) => p.copy(dbName = Some(x)) }
  opt[String]("table-name")                      .required.action { (x, p) => p.copy(tableName = Some(x)) }
  opt[String]("selection")                       .required.action { (x, p) => p.copy(selection = Some(x)) }
  opt[Seq[String]]("grouping")                            .action { (x, p) => p.copy(grouping = x) }
  opt[String]("ouptput-path")                    .required.action { (x, p) => p.copy(outputPath = Some(x)) }
}
// @formatter:on

class Counter extends BaseApplication[CounterParams](CounterParams()) {
  override protected def process(p: CounterParams, sparkSession: SparkSession): Unit = {
    val spark = sparkSession.sqlContext
    val df = spark.read.table(s"${p.dbName.get}.${p.tableName.get}")
      .filter(filterDataFrameWithDatePartitions(p.dateInputs))

    val groupDf =
      if (p.grouping.size == 1)
        df.groupBy(p.grouping.head)
      else
        df.groupBy(p.grouping.head, p.grouping.tail: _*)

    groupDf.agg(countDistinct(p.selection.get))
      .write
      .option("partitionOverwriteMode", "dynamic")
      .mode(SaveMode.Overwrite)
      .parquet(p.outputPath.get)
  }

  override protected def getParser: OptionParser[CounterParams] = CounterParser
}
