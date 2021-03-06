

package clickstream


import java.util.Properties
import config.Settings
import scala.util.Random
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}


object LogProducer extends App {

  /** oto LogProducer- jak nazwa wskazuje jest to program generujący logi imitujące
    *ruch na stronie sklepu internetowego. Część danych jak nazwy producentów jest
    *pobierana z plików CSV a część np. timestamp jest generowana losowo. Wytworzeone
    *tak logi zostaną użyte do przetwarzania w kolejnych etapach projektu*/

  //weblog configuration
  val wlc = Settings.WebLogGen

  //importowanie danych z plikow csv i parsowanie to rzędów i przekształcenie na array
  val Products = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/products.csv")).getLines().toArray
  val Referrers = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/referrers.csv")).getLines().toArray

  //wczytanie gosci z pliku app ( wart int ) stworzenie zakresu od zera do max wartosci
  // i stworzenie dla kazdego goscia stringu "visitor - numer"
  val Visitors = (0 to wlc.visitors).map("Visitor - " + _)
  // to samo dla pages
  val Pages = (0 to wlc.pages).map("Page - " + _)

  //wygenerowanie nowego obiektu random
  val rnd = new Random()

  //obługa Kafki
  val topic = wlc.kafkaTopic
  val props = new Properties()

  //konfiguracja dla KafkaProducera
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.ACKS_CONFIG, "all")  //konfiguracja potwierdzen
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "WebLogProducer") // klient id

  //tworzymy nowa instancje KafkaProducer z ustawieniami z props
  val   kafkaProducer: Producer[Nothing, String]  = new KafkaProducer[Nothing, String] (props)
  println(kafkaProducer.partitionsFor(topic))



//dla zakresu od 1 do wlc.numberOfFiles tworzony jest obiekt FileWriter
//który umozliwia zapisanie do pliku wygenerowanych logów
//
  for (fileCount <- 1 to wlc.numberOfFiles) {
  //  val fw = new FileWriter(filePath, true)

   //randomizacja odstępu czasowego kolejnych klików
    val incrementTimeEvery = rnd.nextInt(wlc.records -1) + 1
    var timestamp = System.currentTimeMillis()
    var adjustedTimestamp = timestamp
// dla wszystkich rekordów(wartość zadana) tworzymy po jednym logu
// log zapisany w stałej line składa się z kolejno tworzonych losowo elementów: adjustedTimeStamp
// referrer, action itd.
  for (iteration <- 1 to wlc.records) {
    adjustedTimestamp = adjustedTimestamp + ((System.currentTimeMillis() - timestamp) * wlc.timeMultiplier)
    timestamp = System.currentTimeMillis() // move all this to a function

    //definicja konkretnych pol rekordu
    val action = iteration % (rnd.nextInt(200) + 1) match {
      case 0 => "purchase"
      case 1 => "add_to_cart"
      case _ => "page_view"
    }
    val referrer = Referrers(rnd.nextInt(Referrers.length - 1))
    val prevPage = referrer match {
      case "Internal" => Pages(rnd.nextInt(Pages.length - 1))
      case _ => ""
    }
    val visitor = Visitors(rnd.nextInt(Visitors.length - 1))
    val page = Pages(rnd.nextInt(Pages.length - 1))
    val product = Products(rnd.nextInt(Products.length - 1))
    //skladanie rekordu w calosc
    val line = s"$adjustedTimestamp\t$referrer\t$action\t$prevPage\t$visitor\t$page\t$product\n"
    val producerRecord = new ProducerRecord(topic, line)
    kafkaProducer.send(producerRecord)


    //fw.write(line)
    //czekaj na produkcje kolejnego loga
    if (iteration % incrementTimeEvery == 0) {
      println(s"Sent $iteration messages!")
      val sleeping = rnd.nextInt(1500)
      println(s"Sleeping for $sleeping ms")
      Thread sleep sleeping
    }

  }
    val sleeping = 2000
    println(s"Sleeping for $sleeping ms")



// zapisywanie danych do plików, nazwy generowane z użyciem "timestamp"

   // val outputFile = FileUtils.getFile(s"${destPath}data_$timestamp")
    //println(s"Moving produced data to $outputFile")
    //FileUtils.moveFile(FileUtils.getFile(filePath), outputFile)
    //val sleeping = 5000
    //println(s"Sleeping for $sleeping ms")

  }

  kafkaProducer.close()
}
