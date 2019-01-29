package clickstream

import config.Settings

object LogProducer extends App {

  //weblog configuration
  val wlc = Settings.WebLogGen
//importowanie danych z plikow csv i parsowanie to rzędów i przekształcenie na array
  val Products = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/products.csv")).getLines().toArray
  val References = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/references.csv")).getLines().toArray
//wczytanie gosci z pliku app ( wart int ) stworzenie zakresu od zera do max wartosci
  // i stworzenie dla kazdego goscia stringu "visitor - numer"
  val Visitors = (0 to wlc.visitors).map("Visitor - " + _)



}
