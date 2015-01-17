val input = sc.textFile("file:///root/dataset/numbers.txt")
input.take(10)
val cachedRDD = input.cache()
val (sum, counts) = cachedRDD.map(
  line => (line.toDouble, 1)
).reduce(
  (a, b) => (a._1 + b._1, a._2 + b._2)
)
val mean = sum / counts
