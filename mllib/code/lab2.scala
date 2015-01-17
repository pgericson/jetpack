import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

// Create a local StreamingContext with two working thread and batch interval of 1 second.
val ssc = new StreamingContext(sc, Seconds(1))

// Create a DStream that will connect to hostname:port, like localhost:9999
val stream = ssc.socketTextStream("localhost", 9999)

val counts = stream.flatMap(_.split(" "))
                   .map(word => (word, 1))
                   .reduceByKey(_ + _)
counts.print()
ssc.start()             // Start the computation
ssc.awaitTermination()  // Wait for the computation to terminate
