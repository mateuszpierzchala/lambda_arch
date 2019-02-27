package utils

import java.lang.management.ManagementFactory

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}


object SparkUtils {
  //zmienna isIDE sprawdza czy pracujemy w srodowisku intellij
  val isIDE = {
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
  }

  def getSparkContext(appName: String) = {
    /* funkcja w której odbywa się konfiguracja i inicjalizacja kontekstu sparka
    * jako argument przyjmuje string appName - nazwę aplikacji */



    var checkpointDirectory = ""

    // konfiguracja sparka
    val conf = new SparkConf()
      .setAppName(appName)

    // sprawdzamy czy pracujemy z IDE- na potreby uruchomienia pod windows
    // trzeba wskazac sciezke winutils
    if (isIDE) {
      System.setProperty("hadoop.home.dir", "C:\\Users\\ADM\\Desktop\\bin\\winutils.exe") // tutaj ścieżka do wintulis //hadoop dla windows nie posiada wszystkich potrzebnych składników dlatego potrzeba pobrania winutils
      conf.setMaster("local[*]")
      checkpointDirectory = "file:///e:/boxes"
    } else {
      checkpointDirectory = "hdfs://lambda-pluralsight:9000/spark/checkpoint"
    }

    // nowy kontekst sparka (lub uzycie juz uruchomionego przez getOrCreate)
    val sc  = SparkContext.getOrCreate(config = conf)
    sc.setCheckpointDir(checkpointDirectory)
    sc

  }

  //nowy sqlcontext (lub uzycie juz uruchomionego przez getOrCreate)
  def getSQLContext(sc: SparkContext) = {
    val sqlContext = SQLContext.getOrCreate(sc)
    sqlContext
  }

  // getStreamingContext pobiera jako parametr funkcję streamingApp która pobiera SparkContext i Duration i zwraca nam Streaming context
  // sprawdza checkpointDir z spark context i jesli istnieje stawia streaming context na istniejacym spark contexcie za pomoca creatingFunc
  // jesli checkpoint nie istnieje po prostu stawia nowy ssc za pomoca cratingFunc
  def getStreamingContext(streamingApp: (SparkContext, Duration) => StreamingContext, sc : SparkContext, batchDuration: Duration) = {
    val creatingFunc : () => StreamingContext = () => streamingApp(sc, batchDuration)
    val ssc= sc.getCheckpointDir match {

      case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir, creatingFunc, sc.hadoopConfiguration, createOnError = true)
      case None => StreamingContext.getActiveOrCreate(creatingFunc)

    }
    sc.getCheckpointDir.foreach(cp => ssc.checkpoint(cp))
    ssc
  }

}
