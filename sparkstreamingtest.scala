import org.apache.spark._  
import org.apache.spark.streaming._  
import org.apache.spark.streaming.StreamingContext._  

object sparkstreamingtest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("sparkstreamingtest")
    val ssc = new StreamingContext(conf, Seconds(1))  
    val lines = ssc.socketTextStream("master", 9999)  
      
    val words = lines.flatMap(_.split(" "))  
    val pairs = words.map(word => (word, 1))  
    val wordCounts = pairs.reduceByKey(_ + _)  
    // Print the first ten elements of each RDD generated in this DStream to the console  
    wordCounts.print()  
      
    ssc.start()             // Start the computation  
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}