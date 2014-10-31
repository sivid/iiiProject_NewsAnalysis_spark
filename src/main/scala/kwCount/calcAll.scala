package kwCount

/**
 * Created by sivid on 10/22/14.
 * outputs a (keyword:String, appeared_in_number_of_documents:Int) list
 * spark-submit --class kwCount.calcAll --name "calcAll" thematrix_2.10-1.0.jar
 * --master yarn-cluster --driver-memory 4g --executor-memory 2g
 * spark-shell --driver-memory 4g --executor-memory 4g --executor-cores 4 --jars thematrix_2.10-1.0.jar --class kwCount.calcAll
 *     --master yarn
 * remember to remove output directory
 *
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._

object calcAll extends App{
  val spConfig = (new SparkConf).setAppName("calcAll")
                //.setMaster( "yarn-cluster" )
                //.setMaster( "yarn-client" )
                //.set("spark.executor.memory", "2g")
                //.set("spark.executor.extraJavaOptions", "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps")
                //.set("spark.shuffle.consolidateFiles", "true")
                //.set("spark.shuffle.manager", "SORT")
                //.set("spark.default.parallelism", "48")
  //   val spConfig = (new SparkConf).set("spark.executor.memory", "1g").set("spark.executor.extraJavaOptions",
  // "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps").set("spark.default.parallelism", "48")
  val sc = new SparkContext(spConfig)
  //val kwLines = sc.textFile("/matrixTest/kw.txt")
  val contentLines = sc.textFile("/user/cloudera/2013-2014/*", 12)
  // val contentLinesB = sc.broadcast(contentLines) // contentLinesB.value   too bad, doesn't work, snappy-java crap
  // val kwLines = sc.textFile("/kw_count_spark/key.txt")
  val kwLines = sc.textFile("/user/cloudera/WordDic.txt")
  //val contentLines = sc.wholeTextFiles("/content_by_month/*") // split by tab, use the third split
  // (filename, content)    org.apache.spark.rdd.RDD[(String, String)] = /content_by_month/* WholeTextFileRDD[2]
  // Array[String] = Array(hdfs://s-hadoopA:9000/content_by_month/020012.txt, hdfs://s-hadoopA:9000/content_by_month/200305.txt, hdfs...
  // contentLines.filter(_._1.toString.contains("200812")).map(_._1).collect+
  // Array[String] = Array(hdfs://s-hadoopA:9000/content_by_month/200812.txt)
  // contentLines.filter(_._1.toString.contains("200812")).map(_._2.split("\t")(2)).first
  // val kwArray = kwLines.collect   // spark doesn't allow rdd-in-rdd loops
  val kwArray = kwLines.map(_.split(' ')(0)).collect
  /*
  val kwCount = contentLines.map(s => s._2.split("\t")(2)).flatMap{ a =>
    kwArray.map{ b =>
      if (a.contains(b)) (b,1)
      else (b,0)
    }
  }.filter(_._2!=0).groupBy(_._1).map(s => (s._1, s._2.size)).map(_.swap).sortByKey().saveAsTextFile("/kw_count_spark/qq")
  */


  // for a single line consisting entirely of content
  val kwCount = contentLines.flatMap{ a =>
    kwArray.map{ b =>
      if (a.contains(b)) (b,1)
      else (b,0)
    }
  }
  // 無限的NPE fuuuuuuuuuuuu
  kwCount.filter(_._2!=0).groupBy(_._1).map(s => (s._1, s._2.size)).map(_.swap).sortByKey().coalesce(1).saveAsTextFile("/user/cloudera/qq1")
  //kwCount.filter(_._2!=0).groupBy(_._1).map(s => (s._1, s._2.size)).map(_.swap).foreach(println)// .saveAsTextFile("/kw_count_spark/qq1")


  /* Example code from http://alvinalexander.com/scala/collection-scala-flatmap-examples-map-flatten
  val chars = 'a' to 'z'
  val perms = chars flatMap { a =>
    chars flatMap { b =>
      if (a != b) Seq("%c%c".format(a, b))
      else Seq()
    }
  }
  val perms1 = chars map { a =>
    chars map { b =>
      if (a != b) Seq("%c%c".format(a, b))
      else Seq()
    }
  }
  */

  // So I went through some hoops trying to solve a non-existent problem... I thought I had something not serializable
  // ==================================PLEASEDISREGARDBELOWCODETHX======================================
  /*
  // method     def someFunc(a: Int) = a + 1
  // function   val someFunc = (a: Int) => a + 1

  val cc = (a:String, b:String) => {
    if (a.contains(b)) (b,1)
    else (b,0)
  }
  val kwCount3 = contentLines.flatMap{ a =>
    kwArray.map(b => cc(a,b))
  }

  // doesn't work
  object coverMe{
    def containMe(a:String, b:String) : (String,Int)= {
      if (a.contains(b)) (b,1)
      else (b,0)
    }
  }
  val kwCount2 = contentLines.flatMap{ a =>
    kwArray.map{ b => coverMe.containMe(a,b)}
  }

  // rdd map genMapper(someFunc)
  def genMapper[A, B](f: A => B): A => B = {
    val locker = com.twitter.chill.MeatLocker(f)
    x => locker.get.apply(x)
  }
  val kwCount1 = contentLines.flatMap{genMapper( a =>
    kwArray.map{ b =>
      if (a.contains(b)) (b,1)
      else (b,0)
    }
    )}
  /*
<console>:24: error: type mismatch;
 found   : String => Array[(String, Int)]
 required: String => TraversableOnce[?]
         val kwCount1 = contentLines.flatMap{genMapper( a =>
                                                      ^
   */

  */

}
