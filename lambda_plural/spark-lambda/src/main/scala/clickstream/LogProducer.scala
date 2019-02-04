package clickstream

import java.io.FileWriter

import config.Settings

import scala.util.Random

object LogProducer extends App {

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
  val filePath = wlc.filePath

  val fw = new FileWriter(filePath, true)

  //randomizacja odstępu czasowego kolejnych klików
  val incrementTimeEvery = rnd.nextInt(wlc.records -1) + 1
  var timestamp = System.currentTimeMillis()
  var adjustedTimeStamp = timestamp

for (iteration <- 1 to wlc.records){
  adjustedTimeStamp = adjustedTimeStamp + ((System.currentTimeMillis()-timestamp) * wlc.timeMultiplier)
  timestamp = System.currentTimeMillis()
  val action = iteration %(rnd.nextInt(200)+1) match {
    case 0 => "purchase"
    case 1 => "add_to_cart"
    case _ => "page_view"
  }
  val referrer = Referrers(rnd)
}
}
