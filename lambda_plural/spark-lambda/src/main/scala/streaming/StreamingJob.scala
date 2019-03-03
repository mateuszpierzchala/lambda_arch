package streaming

import domain.{Activity, ActivityByProduct, VisitorsByProduct}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import utils.SparkUtils._
import functions._
import com.twitter.algebird.HyperLogLogMonoid


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
//sciezki do plikow z logGeneratora
      val inputPath = isIDE match {
        //case true => "E:\\boxes\\spark-kafka-cassandra-applying-lambda-architecture\\lambda_arch\\vagrant\\input"
        case true => "C:\\Users\\mateusz.pierzchala\\OneDrive - Agidens International NV\\studia\\programy\\lambda\\lambda_arch\\vagrant\\input\\"
        case false => "file:///vagrant/input"
      }
//wczytanie streamu ze sciezki i rozbicie za pomoca flatMap
      val textDStream = ssc.textFileStream(inputPath)
      val activityStream = textDStream.transform(input => {
        input.flatMap { line =>
          val record = line.split("\\t")
          val MS_IN_HOUR = 1000 * 60 * 60
          if (record.length == 7)
            Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6)))
          else
            None
        }
      }).cache()



      val activityStateSpec =
        StateSpec
          .function(mapActivityStateFunc)
          .timeout(Minutes(120))

//konwersja do DF
      val statefulActivityByProduct = activityStream.transform(rdd => {
        val df = rdd.toDF()
        df.registerTempTable("activity")
        val activityByProduct = sqlContext.sql("""SELECT
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
          ) } //zwraca slownik K: (product, timestamp) V: (product, timestamp, purchase_count, add_to_cart_count, page_view_count)
        //updateStateByKey pobiera argumenty (Seq[Type], Option[Type](krotka,co bedziemy zwracac) i zwraca stan (Option[Type]
      } ).mapWithState(activityStateSpec)

      // korzytamy tutaj z windowingu(?) do akumulowania wartosci k,v z ActivityByProduct- pozwala to na zmniejszenie potrzebnych zasobow w pamieci poniewaz nie
      //trzeba przechowywac w pamieci calych okien danych ale tylko agregaty z poprzednich okien i elementy które odpadaja i dochodza w kolejnym oknie

      val activityStateSnapshot = statefulActivityByProduct.stateSnapshots()
      activityStateSnapshot
        .reduceByKeyAndWindow(
          (a,b) => a,
          (x,y) => x,
          Seconds(30/4*4)
        ) //zapisuj snapshot co 30 sekund
        .foreachRDD( rdd => rdd.map(sr => ActivityByProduct(sr._1._1, sr._1._2, sr._2._1, sr._2._2, sr._2._3))
        .toDF().registerTempTable("ActivityByProduct"))

        //obliczanie unikatowych uzytkownikow dla danego produktu za pomoca hyperloglog

      val visitorStateSpec =
          StateSpec
              .function(mapVisitorsStateFunc)
              .timeout(Minutes(120))

      val hll = new HyperLogLogMonoid(12)

      val statefulVisitorsByProduct = activityStream.map( a => {
          ((a.product, a.timestamp_hour), hll(a.visitor.getBytes))
        }).mapWithState(visitorStateSpec)


      val visitorStateSnapshot = statefulVisitorsByProduct.stateSnapshots()
      visitorStateSnapshot
          .reduceByKeyAndWindow(
            (a,b) => a,
            (x,y) => x,
            Seconds(30/4*4)
          ) //zapisuj snapshot co 30 sekund
         .foreachRDD( rdd => rdd.map(sr => VisitorsByProduct(sr._1._1, sr._1._2, sr._2.approximateSize.estimate))
                                .toDF().registerTempTable("VisitorsByProduct"))

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
