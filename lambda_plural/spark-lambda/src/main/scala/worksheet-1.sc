val myList = List("Spark", "mimics", "Scala", "collections")
val mapped = myList.map( s => s.toUpperCase )
mapped.foreach(println)
