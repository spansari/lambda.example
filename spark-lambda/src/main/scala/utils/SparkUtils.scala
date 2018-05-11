package utils

import java.lang.management.ManagementFactory

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkUtils {
  val isIDE = {
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
  }

  def getSparkContext(appName: String) = {

    var checkpointDirectory = ""

    val conf = new SparkConf()
      .setAppName(appName)

    if (isIDE) {
      System.setProperty("hadoop.home.dir", "c:\\tools\\WinUtils")
      conf.setMaster("local[*]")
      checkpointDirectory = "file:///c:/Temp"
    } else {
      checkpointDirectory = "hdfs://lambda-pluralsight:9000/spark/checkpoint"
    }

    val sc = SparkContext.getOrCreate(conf)
    sc.setCheckpointDir(checkpointDirectory)
    sc
  }

  def getSQLContext(sc:SparkContext) = {
    val sqlContext = new SQLContext(sc)
    sqlContext
  }
}
