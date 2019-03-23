package batch


import config.Settings
import org.apache.spark.sql.SaveMode
import domain._
import utils.SparkUtils._

/** program wczytujący i przetwarzający dane z generatora logów
  *wczytuje dane jako RDD, następnie rozdziela rekordy i dzieli je na kategorie
  *a następnie wyłuskuje dane za pomoca Spark SQL **/

object BatchJob {
    def main (args: Array[String]): Unit = {

        // inicjalizacja kontekstu sparka

        val sc = getSparkContext("Lambda with Spark")
        val sqlContext = getSQLContext(sc)
        val wlc = Settings.WebLogGen


        // inicjalizacja wejsciowego RDD
        // odczyt plikow parquet z hdfs
        val inputDF = sqlContext.read.parquet(wlc.hdfsPath)
          .where("unix_timestamp() - timestamp_hour / 1000 <= 60 * 60 * 6")

        inputDF.registerTempTable("activity")
        val visitorsByProduct = sqlContext.sql(
            """SELECT product, timestamp_hour, COUNT(DISTINCT visitor) as unique_visitors
              |FROM activity GROUP BY product, timestamp_hour
            """.stripMargin)


      //zapis do cassandry visitorsByProduct
        visitorsByProduct
          .write
          .format("org.apache.spark.sql.cassandra")
          .options(Map( "keyspace" -> "lambda", "table" -> "batch_visitors_by_product"))
          .save()

        val activityByProduct = sqlContext.sql("""SELECT
                                            product,
                                            timestamp_hour,
                                            sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                                            sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                                            sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                                            from activity
                                            group by product, timestamp_hour """).cache()

      //zapis do cassandry activityByProduct
        activityByProduct
          .write
          .format("org.apache.spark.sql.cassandra")
          .options(Map( "keyspace" -> "lambda", "table" -> "batch_activity_by_product"))
          .save()

    }
}
