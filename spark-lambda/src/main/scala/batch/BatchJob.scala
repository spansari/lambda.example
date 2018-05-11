package batch

import java.lang.management.ManagementFactory

import domain.Activity
import org.apache.spark.{SparkConf, SparkContext}

object BatchJob {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark-Lambda")
    if (ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")) {
      System.setProperty("hadoop.home.dir", "C:\\tools\\WinUtils")
      conf.setMaster("local[*]")
    }

    val sc = new SparkContext(conf)

    val sourceFile = "file:///c:/tools/vagrantboxes/spark-kafka-cassandra/vagrant/data.tsv"

    val input = sc.textFile(sourceFile)
    //input.foreach(println)

    val inputRDD = input.flatMap{ line =>
      val record = line.split("\\t")
      val MS_IN_HOUR = 100 * 60 * 60
      if (record.length == 7)
        Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6) ))
      else
        None
    }

    val keyedByProduct = inputRDD.keyBy(a => (a.product, a.timestamp_hour)).cache()

    val visitorsByProduct = keyedByProduct
      .mapValues(a => a.visitor)
      .distinct()
      .countByKey()

    visitorsByProduct.foreach(println)

    val activityByProduct = keyedByProduct
      .mapValues(a =>
        a.action match {
          case "purchase" => (1,0,0)
          case "add_to_cart" => (0,1,0)
          case "page_view" => (0,0,1)
        }
      )
      .reduceByKey( (a,b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3) )

    activityByProduct.foreach(println)

  }
}
