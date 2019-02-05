package batch

import org.apache.spark.sql.SaveMode
import domain._
import utils.SparkUtils._

/**wersja próbna programu**/
object BatchJob {
    def main (args:Array(String)): Unit ={

        //konfiguracja sparka
        val conf = new SparkConf().setAppName("Lambda_arch")

        // sprawdzenie czy startujemy z IDE
        if(ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")) {
            System.setProperty("hadoop.home.dir", "F:\\Libraries\\WinUtils") // musi być zainstalowane winutils z:
            conf.setMaster("local[*]")
        }

        // ustawienie SparkContext ze stringiem konfiguracyjnym
        val sc = new SparkContext(conf)
        
        // inicjalizacja danych wejsciowych (zostaną wczytane jako RDD) 
        // lazy eval
        val sourceFile = "file:///c:/tmp/spark-test/data.tsv"
        val input = sc.textFile(sourceFile)

        // wywołanie akcji sparka dla kolejnych elem (wypisanie znaku kolejnej linii) (lazy eval)
        input.foreach(println)
    }   
}