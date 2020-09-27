package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import annotation.tailrec
import scala.reflect.ClassTag
import java.util.Calendar

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]) extends Serializable


/** The main class */
object StackOverflow extends StackOverflow {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    println("lines type", lines.getClass.getName)         // org.apache.spark.rdd.MapPartitionsRDD
    lines.take(10).foreach(println)                       // 1,27233496,,,0,C#

    val raw     = rawPostings(lines)
    println("raw type", lines.getClass.getName)           // type,org.apache.spark.rdd.MapPartitionsRDD
    raw.take(5).foreach(println)                          // Posting(1,27233496,None,None,0,Some(C#))
    raw.take(1).foreach(x => println(x.postingType))      // 1
    raw.take(1).foreach(x => println(x.id))               // 27233496

    val grouped = groupedPostings(raw)
    grouped.take(5).foreach(println)

    val scored  = scoredPostings(grouped).sample(true, 0.001, 0)    // no .sample                                                // .sample(true, 0.001, 0) -> smallest that not error
    scored.take(5).foreach(println)                                 // (Posting(1,13882656,None,None,-1,Some(PHP)),5)            // (Posting(1,22749348,None,None,0,Some(C++)),0)
    println(scored.count)                                           // 2121822                                                   // 2134

    val vectors = vectorPostings(scored)
    vectors.take(5).foreach(println)                                // (100000,5) -> PHP index 2 * langSpread 50000 = 100000     // (250000,0) -> C++ index 5 * langSpread 50000 = 250000
    println(vectors.count)                                          // 2121822                                                   // 2134
//    assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())

    println(sampleVectors(vectors).size)                            // 45

    println("time-start-kmeans", Calendar.getInstance().getTime())  // 11:47:49                                                  // 11:29:19
    val means = kmeans(sampleVectors(vectors), vectors, debug = true)
//    println("means", means.getClass.getName)
//    means.foreach(println)
//    val means = Array((450000,1), (450000,10), (450000,105), (0,2), (0,2309), (0,467), (600000,7), (600000,1), (600000,0), (150000,1629), (150000,3), (150000,292), (300000,105), (300000,3), (300000,557), (50000,10271), (50000,334), (50000,2), (200000,2), (200000,99), (200000,530), (500000,176), (500000,31), (500000,3), (350000,940), (350000,2), (350000,210), (650000,2), (650000,74), (650000,15), (100000,1263), (100000,182), (100000,2), (400000,2), (400000,121), (400000,584), (550000,5), (550000,1130), (550000,66), (250000,1766), (250000,271), (250000,3), (700000,4), (700000,0), (700000,49))
    println("time-finish-kmeans", Calendar.getInstance().getTime()) // 12:53:34                                                  // 11:35:23

    val results = clusterResults(means, vectors)
    printResults(results)
  }
}

/** The parsing and kmeans methods */
class StackOverflow extends StackOverflowInterface with Serializable {

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
              id =             arr(1).toInt,
              acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
              parentId =       if (arr(3) == "") None else Some(arr(3).toInt),
              score =          arr(4).toInt,
              tags =           if (arr.length >= 6) Some(arr(5).intern()) else None)
    })


  /** Group the questions and answers together */
  def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] = {
    val question = postings.filter(x => x.postingType == 1).map(x => (x.id, x))
    question.take(1).foreach(println)                     // (27233496,Posting(1,27233496,None,None,0,Some(C#)))
    println("question type", question.getClass.getName)   // org.apache.spark.rdd.MapPartitionsRDD
    println("question count", question.count())           // 3983281

    val answer = postings.filter(x => x.postingType == 2).map(x => (x.parentId.get, x))
    answer.take(1).foreach(println)                       // (5484340,Posting(2,5494879,None,Some(5484340),1,None))
    println("answer type", answer.getClass.getName)       // org.apache.spark.rdd.MapPartitionsRDD
    println("answer count", answer.count())               // 4160520

    val qa = question.join(answer)
    qa.take(1).foreach(println)                           // (18583638,(Posting(1,18583638,None,None,-1,Some(Objective-C)),Posting(2,18583768,None,Some(18583638),1,None)))
    println("qa count", qa.count())                       // 4160520

    qa.groupByKey()                                       // (13882656,CompactBuffer(
                                                          //            (Posting(1,13882656,None,None,-1,Some(PHP)),Posting(2,13882679,None,Some(13882656),2,None)),
                                                          //            (Posting(1,13882656,None,None,-1,Some(PHP)),Posting(2,13882685,None,Some(13882656),2,None)),
                                                          //            (Posting(1,13882656,None,None,-1,Some(PHP)),Posting(2,13882687,None,Some(13882656),5,None)),
                                                          //            (Posting(1,13882656,None,None,-1,Some(PHP)),Posting(2,13882700,None,Some(13882656),0,None)),
                                                          //            (Posting(1,13882656,None,None,-1,Some(PHP)),Posting(2,13882735,None,Some(13882656),0,None)),
                                                          //            (Posting(1,13882656,None,None,-1,Some(PHP)),Posting(2,13884757,None,Some(13882656),0,None))
                                                          // ))
  }


  /** Compute the maximum score for each posting */
  def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] = {
    def answerHighScore(as: Array[Answer]): HighScore = {
      var highScore = 0
          var i = 0
          while (i < as.length) {
            val score = as(i).score
                if (score > highScore)
                  highScore = score
                  i += 1
          }
      highScore
    }
    grouped.map(x => (x._2.toList(0)._1, answerHighScore(x._2.map(y => y._2).toArray)))
  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)] = {
    /** Return optional index of first language that occurs in `tags`. */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag.get == ls.head) Some(0) // index: 0
      else {
        val tmp = firstLangInTag(tag, ls.tail)
        tmp match {
          case None => None
          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
        }
      }
    }
    scored.map(x => (firstLangInTag(x._1.tags, langs).get * langSpread, x._2)).cache()
  }


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
        // sample the space regardless of the language
        vectors.takeSample(false, kmeansKernels, 42)
      else
        // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }


  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {
//    val newMeans = means.clone() // you need to compute newMeans

    if (iter == 1) println("----- kmeans-means", means.length)                         // (----- kmeans-means,45)
    if (iter == 1) means.take(5).foreach(println)                                      // (450000,1)
    if (iter == 1) means.indices.take(5).foreach(println)                              // 0 -> 1 -> 2 -> 3 -> 4 -> ...
    val closest = vectors.map(p => (findClosest(p, means), p))
    if (iter == 1) println("----- kmeans-closest", closest.count())                    // (----- kmeans-closest,2134)
    if (iter == 1) closest.take(5).foreach(println)                                    // (30,(100000,1))
    val closestGrouped = closest.groupByKey()
    if (iter == 1) println("----- kmeans-closestGrouped", closestGrouped.count())      // ----- kmeans-closestGrouped,38)
    if (iter == 1) closestGrouped.take(5).foreach(println)                             // (30,CompactBuffer((100000,1), (100000,1), ...))

    val avg = closestGrouped.mapValues(averageVectors)
    if (iter == 1) println("----- kmeans-avg", avg.count())                            // (----- kmeans-avg,38)
    if (iter == 1) avg.foreach(println)                                                // (30,(100000,0))
    val avgMap = avg.collectAsMap()
    if (iter == 1) println("----- kmeans-avgMap", avgMap.size)                         // (----- kmeans-avgMap,38)
    if (iter == 1) avgMap.foreach(println)                                             // (30,(100000,0))
    val newMeans = means.indices.map(i => avgMap.getOrElse(i, means(i))).toArray
    if (iter == 1) println("----- kmeans-newMeans", newMeans.length)                   // (----- kmeans-newMeans,45)
    if (iter == 1) newMeans.take(5).foreach(println)                                   // (450000,3)

    // TODO: Fill in the newMeans array
    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"Iteration: $iter")
      if (iter == 1) println(s"""  * current distance: $distance
                                |  * desired distance: $kmeansEta
                                |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
        if (iter == 1) println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
              f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      if (debug) {
        println("Reached max iterations!")
      }
      newMeans
    }
  }


  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }


  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)] = {
                                                                                      // no .sample                                   // .sample(true, 0.001, 0) -> smallest that not error
    println("----- clusterResults-means", means.length)                               // (----- clusterResults-means,45)              // (----- clusterResults-means,45)
    means.take(5).foreach(println)                                                    // (450000,1)                                   // (450000,1)
    means.indices.take(5).foreach(println)                                            // 0 -> 1 -> 2 -> 3 -> 4 -> ...                 // 0 -> 1 -> 2 -> 3 -> 4 -> ...
    val closest = vectors.map(p => (findClosest(p, means), p))
    println("----- clusterResults-closest", closest.count())                          // (----- clusterResults-closest,2121822)       // (----- clusterResults-closest,2134)
    closest.take(5).foreach(println)                                                  // (10,(150000,0))                              // (41,(250000,0))
    val closestGrouped = closest.groupByKey()
    println("----- clusterResults-closestGrouped", closestGrouped.count())                                                            // (----- clusterResults-closestGrouped,26)
    closestGrouped.foreach(println)                                                                                                   // (6,CompactBuffer((600000,5), (600000,10), (600000,11), (600000,4)))

    val median = closestGrouped.mapValues { vs =>
      val groupByIndex = vs.groupBy(_._1)                                             // Map(0 -> List((0,289), (0,255), ...)         // Map(600000 -> List((600000,5), (600000,10), (600000,11), (600000,4)))
      val groupByIndexSize = groupByIndex.mapValues(_.size)                           // Map(0 -> 431)                                // Map(600000 -> 4)
      val groupByIndexSizeMax = groupByIndexSize.maxBy(_._2)                          // (0,431)                                      // (600000,4)

      val langLabel: String   = langs(groupByIndexSizeMax._1  / langSpread)           // JavaScript -> 0 = 0/50000                    // MATLAB -> 12 = 600000/50000   // most common language in the cluster
      val langPercent: Double = groupByIndexSizeMax._2 * 100.0 / vs.size.toDouble     // 100.0 -> 431*100/431                         // 100.0 -> 4*100/4              // percent of the questions in the most common language
      val clusterSize: Int    = vs.size                                               // 431                                          // 4

      val allScores = vs.map(_._2).toList.sorted                                      // List(235, 235, ...)                          // List(4, 5, 10, 11)
      val medianScore: Int    = allScores.size%2 match {                              // 431/2 = 215                                  // 4/2 = 2
        case 1 => allScores(allScores.size/2)                                         // allScores(215) = 377                         // NA
        case _ => (allScores(allScores.size/2) + allScores(allScores.size/2 -1))/2    // NA                                           // (allScores(2) + allScores(1))/2 = (10 + 5)/2 = 7
      }
      println(s"""-----
                  |  $groupByIndex
                  |  $groupByIndexSize
                  |  $groupByIndexSizeMax
                  |  $langLabel
                  |  $langPercent
                  |  $clusterSize
                  |  $allScores
                  |  $medianScore""".stripMargin)
      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }
}
