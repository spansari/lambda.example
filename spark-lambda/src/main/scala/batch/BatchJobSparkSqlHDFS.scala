package batch

import java.lang.management.ManagementFactory

import domain.Activity
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object BatchJobSparkSqlHDFS {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark-Lambda")
    if (ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")) {
      System.setProperty("hadoop.home.dir", "C:\\tools\\WinUtils")
      conf.setMaster("local[*]")
    }

    val sc = new SparkContext(conf)

    implicit val sqlContext = new SQLContext(sc)

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val sourceFile = "file:///vagrant/data.tsv"

    val input = sc.textFile(sourceFile)
    //input.foreach(println)

    val inputDF = input.flatMap{ line =>
      val record = line.split("\\t")
      val MS_IN_HOUR = 100 * 60 * 60
      if (record.length == 7)
        Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6) ))
      else
        None
    }.toDF()

    val df = inputDF.select(
      add_months(from_unixtime(inputDF("timestamp_hour")/1000), 1).as("timestamp_hour"),
      inputDF("referrer"),inputDF("action"),inputDF("prevPage"),inputDF("page"),inputDF("visitor"),inputDF("product")
    ).cache()

    df.registerTempTable("activity")

    val visitorByProduct = sqlContext.sql(
      """SELECT product, timestamp_hour, COUNT(DISTINCT visitor) as unique_visitors
        | FROM activity GROUP BY product, timestamp_hour
      """.stripMargin)

    visitorByProduct.foreach(println)

    val activityByProduct = sqlContext.sql(
      """SELECT
        |product,
        |timestamp_hour,
        |sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
        |sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
        |sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
        |from activity
        |group by product, timestamp_hour
      """.stripMargin).cache()

    activityByProduct.write.partitionBy("timestamp_hour").mode(SaveMode.Append).parquet("hdfs://lambda-pluralsight:9000/lambda/batch1")

    activityByProduct.foreach(println)

  }
}
