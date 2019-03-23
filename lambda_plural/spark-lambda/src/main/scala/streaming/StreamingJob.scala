package streaming


import domain.{Activity, ActivityByProduct, VisitorsByProduct}
import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StateSpec, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import utils.SparkUtils._
import functions._
import com.twitter.algebird.HyperLogLogMonoid
import config.Settings
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.apache.spark.sql.SaveMode

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

import scala.util.Try

//noinspection ScalaUnnecessaryParentheses
object StreamingJob {
  def main(args: Array[String]): Unit = {
    // ustawiamy kontekst sparka
    val sc = getSparkContext("Lambda with Spark")
    val sqlContext = getSQLContext(sc)
    import sqlContext.implicits._
    // ustawiamy czas trwania jednego batcha (tj. kazdy RDD bedzie zawieral dane wygenerowane w przeciągu okna czasowego 4s)
    val batchDuration = Seconds(4)

    def streamingApp(sc: SparkContext, batchDuration: Duration) = {
      /** funkcja pobierajace spark context i tworzaca na jego podstawie streamingContext
        * nastepnie pobierajaca dane wygenerowane z log generatora transformujaca do dataFrame i przetwarzajaca za pomoca squery SQL
        */


      val ssc = new StreamingContext(sc, batchDuration)
      val wlc = Settings.WebLogGen
      val topic = wlc.kafkaTopic

//      //konfiguracja kafki do pracy w trybie reciver based approach
//      val kafkaParams = Map(
//        "zookeeper.connect" -> "localhost:2181",
//        "group.id" -> "lambda",
//        "auto.offset.reset" ->"largest"
//      )
      //  konfiguracja kafki do pracy w trybie direct streaming
      val kafkaDirectParams = Map(
        "metadata.broker.list" -> "localhost:9092",
        "group.id" -> "lambda",
        "auto.offset.reset" -> "largest"
      )
//troche konfiguracji do zapisywania na hdfs
      var fromOffsets : Map[TopicAndPartition, Long] = Map.empty
      val hdfsPath = wlc.hdfsPath

      Try(sqlContext.read.parquet(hdfsPath)).foreach( hdfsData =>
        fromOffsets = hdfsData.groupBy("topic", "kafkaPartition").agg(max("untilOffset").as("untilOffset"))
          .collect().map { row =>
          (TopicAndPartition(row.getAs[String]("topic"), row.getAs[Int]("kafkaPartition")), row.getAs[String]("untilOffset").toLong + 1)
        }.toMap
      )

      val kafkaDirectStream = fromOffsets.isEmpty match {
        case true =>
          KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc, kafkaDirectParams, Set(topic)
          )
        case false =>
          KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
            ssc, kafkaDirectParams, fromOffsets, { mmd : MessageAndMetadata[String, String] => (mmd.key(), mmd.message()) }
          )
      }

      val activityStream = kafkaDirectStream.transform(input => {
        functions.rddToRDDActivity(input)
      }).cache()

//zapisywanie na hdfs
      activityStream.foreachRDD { rdd =>
        val activityDF = rdd
          .toDF()
          .selectExpr("timestamp_hour", "referrer", "action", "prevPage", "page", "visitor", "product", "inputProps.topic as topic", "inputProps.kafkaPartition as kafkaPartition", "inputProps.fromOffset as fromOffset", "inputProps.untilOffset as untilOffset")

        activityDF
          .write
          .partitionBy("topic", "kafkaPartition", "timestamp_hour")
          .mode(SaveMode.Append)
          .parquet(hdfsPath)
      }



      val activityStateSpec =
        StateSpec
          .function(mapActivityStateFunc)
          .timeout(Minutes(120))

//konwersja do DF
//zwraca slownik K: (product, timestamp) V: (product, timestamp, purchase_count, add_to_cart_count, page_view_count)
//updateStateByKey pobiera argumenty (Seq[Type], Option[Type](krotka,co bedziemy zwracac) i zwraca stan (Option[Type]
      val statefulActivityByProduct = activityStream.transform(rdd => {
        val df = rdd.toDF()
        df.registerTempTable("activity")
        val activityByProduct = sqlContext.sql(
          """SELECT
                    product,
                    timestamp_hour,
                    sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                    sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                    sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                    from activity
                    group by product, timestamp_hour """)
        activityByProduct
          .map { r => ((r.getString(0), r.getLong(1)),
          ActivityByProduct(r.getString(0), r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4))
          )
          }
      }).mapWithState(activityStateSpec)

      // korzytamy tutaj z windowingu(?) do akumulowania wartosci k,v z ActivityByProduct- pozwala to na zmniejszenie potrzebnych zasobow w pamieci poniewaz nie
      //trzeba przechowywac w pamieci calych okien danych ale tylko agregaty z poprzednich okien i elementy które odpadaja i dochodza w kolejnym oknie


      val activityStateSnapshot = statefulActivityByProduct.stateSnapshots()
      activityStateSnapshot
        .reduceByKeyAndWindow(
          (a, b) => b,
          (x, y) => x,
          Seconds(30 / 4 * 4)
        ) // only save or expose the snapshot every x seconds
        .map(sr => ActivityByProduct(sr._1._1, sr._1._2, sr._2._1, sr._2._2, sr._2._3))
        .saveToCassandra("lambda", "stream_activity_by_product")


      //obliczanie unikatowych uzytkownikow dla danego produktu za pomoca hyperloglog

      val visitorStateSpec =
          StateSpec
              .function(mapVisitorsStateFunc)
              .timeout(Minutes(120))

      val statefulVisitorsByProduct = activityStream.map(a => {
        val hll = new HyperLogLogMonoid(12)
        ((a.product, a.timestamp_hour), hll(a.visitor.getBytes))
      }).mapWithState(visitorStateSpec)


      val visitorStateSnapshot = statefulVisitorsByProduct.stateSnapshots()
      visitorStateSnapshot
        .reduceByKeyAndWindow(
          (a, b) => b,
          (x, y) => x,
          Seconds(30 / 4 * 4),
          filterFunc = (record) => false
        ) // only save or expose the snapshot every x seconds
        .map(sr => VisitorsByProduct(sr._1._1, sr._1._2, sr._2.approximateSize.estimate))
        .saveToCassandra("lambda", "stream_visitors_by_product")

      //updateStateByKey - troche inna wersja
      /*.updateStateByKey((newItemsPerKey: Seq[ActivityByProduct],currentState: Option[(Long, Long, Long, Long)]) =>  {
      //zwraca poprzednie wartosci purchase_count.... lub zera jesli jeszcze nie istnieja
      var (prevTimestamp, purchase_count, add_to_cart_count, page_view_count) = currentState.getOrElse((System.currentTimeMillis(),0L,0L,0L))
      var result: Option[(Long, Long, Long, Long)] = null
      if (newItemsPerKey.isEmpty){
        //jesli jakis klucz znajduje sie w naszym stanie ponad 30s + okno batcha usuwamy go
        if(System.currentTimeMillis() - prevTimestamp > 30000 + 4000)
          result = None
        else
          result = Some((prevTimestamp, purchase_count, add_to_cart_count, page_view_count) )

      } else {
//dla kazdego newItemsPerKey sumujemy wartosc aktualna z suma wartosci poprzednich
        newItemsPerKey.foreach(a => {
        purchase_count += a.purchase_count
        add_to_cart_count += a.add_to_cart_count
        page_view_count += a.page_view_count
      })

      result = Some(( System.currentTimeMillis(), purchase_count, add_to_cart_count, page_view_count))

        }
      result })
*/




      ssc
    }

    val ssc = getStreamingContext(streamingApp, sc, batchDuration)
    ssc.start()
    ssc.awaitTermination()

  }

}
