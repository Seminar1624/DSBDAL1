
# go to home -> right click on spark tar.gz -> extract here 
-  click on that spark, go to bin, in bin right click and click on "open in terminal"
- open new terminal
- so u have 2 terminals one spark and other simple home

# on regular terminal
- nc -lk 9999

#go to text editor and paste the code, name the file as word.scala

# on spark terminal
-  ./spark-shell -i /home/pict/word.scala

# on regular terminal - enter words , and check word count on spark terminal
### code

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession
 .builder
 .appName("StructuredNetworkWordCount")
 .master("local[*]")
 .getOrCreate()

import spark.implicits._

val lines = spark.readStream
 .format("socket")
 .option("host", "localhost")
 .option("port", 9999)
 .load()

val words = lines.as[String].flatMap(_.split(" "))

val wordCounts = words.groupBy("value").count()

val query = wordCounts.writeStream
 .outputMode("complete")
 .format("console")
 .start()

query.awaitTermination()




###bubble sort
import org.apache.spark.sql.SparkSession

// Create Spark Session
val spark = SparkSession
  .builder
  .appName("BubbleSortExample")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._

// Sample data: unsorted list of numbers
val data = Seq(34, 12, 5, 66, 23, 1, 99, 4)

// Convert data to Dataset
val ds = spark.createDataset(data)

// Bubble Sort Function
def bubbleSort(list: Seq[Int]): Seq[Int] = {
  val arr = list.toArray
  for (i <- arr.indices) {
    for (j <- 0 until arr.length - i - 1) {
      if (arr(j) > arr(j + 1)) {
        val temp = arr(j)
        arr(j) = arr(j + 1)
        arr(j + 1) = temp
      }
    }
  }
  arr.toSeq
}

// Apply bubble sort
val sortedDS = ds.collect()   // bring data to driver
val sortedList = bubbleSort(sortedDS)

// Display result
println("Sorted List using Bubble Sort:")
sortedList.foreach(println)

// Stop Spark Session
spark.stop()

