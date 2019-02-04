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
  val destPath = wlc.destPath

  for (fileCount <- 1 to wlc.numberOfFiles) {
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
      val referrer = Referrers(rnd.nextInt(Referrers.length -1))
      val prevPage = referrer match {
        case "Internal" => Pages(rnd.nextInt(Pages.length -1))
        case _ => ""
      }

      val visitor = Visitors(rnd.nextInt(Visitors.length -1))
      val page = Pages(rnd.nextInt(Pages.length -1))
      val product = Products(rnd.nextInt(Products.length -1))

      val line = s"%adjustedTimestamp\t$referrer\t$action\t$prevPage\t$visitor\t$page\t$product\n"
      fw.write(line)

      if (iteration % incrementTimeEvery ==0) {
        println(s"Sent $iteration messages!")
        val sleeping = rnd.nextInt(incrementTimeEvery *60)
        println(s"Sleeping for $sleeping ms")
        Thread sleep sleeping 
      }

    }
    fw.close()

    val outputFile = FileUtils.getFile(s"${destPath}data_$timestamp")
    println(s"Moving produced data to $outputFile")
    FileUtils.moveFile(FileUtils.getFile(filePath), outputFile)
    val sleeping = 5000
    println(s"Sleeping for $sleeping ms")

  }
}
