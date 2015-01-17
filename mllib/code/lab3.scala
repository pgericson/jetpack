    import org.apache.spark._
    import org.apache.spark.streaming._
    import org.apache.spark.streaming.dstream.DStream
    import org.apache.spark.streaming.{StreamingContext, Seconds}
    import org.apache.spark.streaming.StreamingContext._

    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint("file:///root")

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val stream = ssc.socketTextStream("localhost", 9999)

    val intervalResult: DStream[(Double, Long)] = stream.flatMap(
      line =>
        try {
          Some(line.toDouble, 1L)
        } catch {
          case _ : Throwable => None
        }
    ).reduce((a, b) => (a._1 + b._1, a._2 + b._2))

    def updateFunc =
      (currentState: Seq[(Double, Long)], previousState: Option[(Double, Long)]) => {
        val (previousSum, previousCounts) = previousState.getOrElse((0.0, 0L))
        val (currentSum, currentCounts) = {
          var sum = 0.0
          var counts = 0L
          // In theory, currentSate will only have one element.
          currentState.map { x =>
            sum += x._1
            counts += x._2
          }
          (sum, counts)
        }
        Some((previousSum + currentSum, previousCounts + currentCounts))
      }

    val allResult: DStream[(Double, Long)] = intervalResult
      .map(x => ("key1", x))
      .updateStateByKey[(Double, Long)](updateFunc)
      .map(x => x._2)

    intervalResult.map(x =>
      "sum during (t-1, t): " + x._1 + "\n" +
      "counts during (t-1, t): " + x._2 + "\n" +
      "mean during (t-1, t): " + x._1 / x._2
    ).union(allResult.map(x =>
      "sum during (0, t): " + x._1 + "\n" +
      "counts during (0, t): " + x._2 + "\n" +
      "mean during (0, t): " + x._1 / x._2
    )).print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

