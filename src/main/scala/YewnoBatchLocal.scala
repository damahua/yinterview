import java.io._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex
import scala.io.{Codec, Source}

/**
  * Code for local environment
  * Can be run on local machine
  *
  * See result in temp directory
  *
  * Created by lzhang on 1/27/17.
  */
object YewnoBatchLocal {
  def main(args: Array[String]) {
    val action = args(0)
    val master = args(1)
    val conf = new SparkConf().setAppName("Yewno")
    if (master.equalsIgnoreCase("local"))
      conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    action match {
      case "combine" => {
        val target = args(3)
        val dir = args(2)
        combineFiles(dir, target, sc)
        println("Combine File Successfully! New file is located at " + target)
      }
      case "sim" => {
        val target = args(2)
        val output = args(3)
        val unit = args(4)
        val status = simFun(sc, target, output, unit)
        if (status)
          println("Successfully generate file containing similarities of word pairs at " + output)
        else
          println("Fail in generating files at " + output)
      }
    }
    sc.stop()
  }

  private def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  /**
    * combine all of files under the "dir" directory to one single file located at "target"
    *
    * @param dir    directory where files are located
    * @param target targeting location for combined file
    * @param sc     sparkContext used for reading and writing files
    */
  def combineFiles(dir: String, target: String, sc: SparkContext): Unit = {
    //    def combineFiles(dir: String, target: String): List[(Text, Text)] = {
    val list = getListOfFiles(dir).map({ x: File =>
      val name = x.getName
      val source = Source.fromFile(x.getAbsolutePath)(Codec.ISO8859)
      try {
        val content = source.mkString
        (name, content)
      } finally {
        source.close()
      }
    })
    sc.parallelize(list).saveAsSequenceFile(target)
  }

  /**
    *
    * @param sc     sparkContext used for reading and writing files
    * @param target where combined file located
    * @param output the directory used for storing generated files(num.dat, single.dat, pairs.dat, similarities.dat)
    * @param unit   the basic unit for counting (per post or per sentence)
    * @return
    */
  def simFun(sc: SparkContext, target: String, output: String, unit: String): Boolean = {
    try {
      val rdd = sc.sequenceFile[String, String](target).map({
        case (a, b) => getTuple(a, b, unit)
      })

      // number of units
      val all = rdd.flatMap(x => x).cache()
        .map({ case (a: Int, b: Array[((String, String), Int)], c: Array[(String, Int)]) => (a) })
        .collect().reduce(_ + _)
      unit match {
        case "post" => println("Number of posts is " + all)
        case "sentence" => println("Number of sentence is " + all)
      }
      sc.parallelize(Array(all)).repartition(1).saveAsTextFile(output + "/num.dat")
      // number of single distince word
      val oSingle = rdd.flatMap(x => x).map({ case (a: Int, b: Array[((String, String), Int)], c: Array[(String, Int)]) => c })
        .flatMap(x => x)
        .reduceByKey((a, b) => a + b)
        .collect()
      println("Number of words is " + oSingle.size)
      sc.parallelize(oSingle).repartition(1).saveAsTextFile(output + "/single.dat")

      // number of pairs
      val oTwo = rdd.flatMap(x => x).map({ case (a: Int, b: Array[((String, String), Int)], c: Array[(String, Int)]) => b })
        .flatMap(x => x)
        .reduceByKey(_ + _)
        .collect()
      println("Number of pairs is" + oTwo.length)
      sc.parallelize(oTwo).repartition(1).saveAsTextFile(output + "/pairs.dat")

      // map for word and number of occurrences of word
      // pword = (word, (#Word, #^word))
      val pword = oSingle.map({ case (word, num) => (word, (num, all - num)) })(collection.breakOut): Map[String, (Int, Int)]
      // ((wordA, wordB), ((#wordA, #^wordA), (#wordB, #^wordB), #Pair, #^Pair))
      val ppair: Map[(String, String), ((Int, Int), (Int, Int), Int, Int)] = oTwo.map { case ((wordA, wordB), num) => ((wordA, wordB), (pword(wordA), pword(wordB), num, all - num)) }(collection.breakOut)
      val p = ppair.map(x => {
        //case 1
        // p(wordA, wordB)
        val case1 = (x._2._3 / all.toDouble) * math.log((x._2._3 / all.toDouble) / ((x._2._1._1 / all.toDouble) * (x._2._2._1 / all.toDouble)))
        // p(wordA, ^wordB)
        val case2 = ((x._2._1._1 - x._2._3) / all.toDouble) * math.log((x._2._1._1 - x._2._3 / all.toDouble) / ((x._2._1._1 / all.toDouble) * (x._2._2._2 / all.toDouble)))
        // p(^wordA, wordB)
        val case3 = ((x._2._2._1 - x._2._3) / all.toDouble) * math.log((x._2._2._1 - x._2._3 / all.toDouble) / ((x._2._1._2 / all.toDouble) * (x._2._2._1 / all.toDouble)))
        // p(^wordA, ^wordB)
        val case4 = ((all - x._2._3) / all.toDouble) * math.log((all - x._2._3 / all.toDouble) / ((x._2._1._2 / all.toDouble) * (x._2._2._2 / all.toDouble)))
        //((wordA, wordB), sim)
        (x._1, case1 + case2 + case3 + case4)
      })
      sc.parallelize(p.toList).repartition(1).saveAsTextFile(output + "/similarities.dat")
      true
    }
    catch {
      case ex: Exception => {
        println("an exception happened." + ex.getMessage)
        false
      }
    }
  }

  implicit def addComb(list: List[String]) = new ListCombination(list)

  /**
    * generate a list of combination of 2 words for a list of words
    *
    * @param list list where words are from
    */
  case class ListCombination(list: List[String]) {
    def comb(num: Int): List[(String, String)] = {
      //      var rst = collection.mutable.ListBuffer[String]()
      val sortedList = list.sorted
      for (a <- sortedList; b <- sortedList if !a.equals(b))
        yield (a, b)
    }
  }

  /**
    * count the number of units
    * the number of single word
    * the number of pairs
    * per file or per blogger
    *
    * @param key   the key for each entry in sequence file. The original file name.
    * @param value the value for each entry in sequence file. The content of the blogs from the original file.
    * @param unit  the basic unit for counting (post or sentence)
    * @return (number of units, Array of tuples which consists of a paire of words and number of occurrence of this pair
    *         per unit, Array of tuples which consists of the word and the number of occurrence of this word per unit)
    */
  def getTuple(key: String, value: String, unit: String): Array[(Int, Array[((String, String), Int)], Array[(String, Int)])] = {
    val pattern = new Regex("<post>[\\s\\S]*?</post>")
    val tuples = pattern.findAllIn(value.toString).toArray.map(blog => {
      unit match {
        case "post" => {
          // count per post
          val temp = blog.stripPrefix("<post>").stripSuffix("</post>").trim.split(" ")
            .map(x => "[a-zA-Z0-9]*".r.findFirstIn(x.trim) match {
              case Some(word) => word
              case None => ""
            }).distinct
            .filter(!_.isEmpty)
          val combinationOf2 = temp.toList.comb(2).map({ case (a, b) => ((a, b), 1) })
          val singleWord = temp.map(x => (x, 1))
          (1, combinationOf2.toArray, singleWord.toArray)
        } // Array[(Int, Array[((String, String), Int)], Array[(String, Int)])]
        case "sentence" => {
          // count per sentence
          blog.stripPrefix("<post>").stripSuffix("</post>").trim.split('.').map(s => {
            val temp = s.split(" ")
              .map(w => "[a-zA-Z0-9]*".r.findFirstIn(w.trim) match {
                case Some(word) => word
                case None => ""
              }).distinct
              .filter(!_.isEmpty)
            val combinationOf2 = temp.toList.comb(2).map({ case (a, b) => ((a, b), 1) })
            val singleWord = temp.map(word => (word, 1))
            (1, combinationOf2.toArray, singleWord.toArray)
          }).foldLeft((0, Array.empty[((String, String), Int)], Array.empty[(String, Int)]))((a, b) => (a._1 + b._1, a._2 ++ b._2, a._3 ++ b._3))
        }
      }
    })
    tuples
  }
}
