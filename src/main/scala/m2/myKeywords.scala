package m2

/**
 * Created by sivid on 10/15/14.
 * $HADOOP_HOME/sbin/start-dfs.sh;$HADOOP_HOME/sbin/start-yarn.sh;$SPARK_HOME/sbin/start-all.sh
 * http://btsxj.wordpress.com/2014/08/17/multiplication-of-matrices-on-spark-mllib/
 * sbt package
 * hh -rm -r /matrixTest/t
 * spark-submit --class m2.myKeywords --master local[4] thematrix_2.10-1.0.jar
 *
 * This program takes two files as input,
 * 1. contentLines: one news content per line
 * 2. kwLines: one keyword per line
 * and creates a "matrix" that specifies how many times each keyword has appeared with each other.
 * For further functions please see each functions' comment.
 */

import java.io.{ObjectOutputStream, FileOutputStream}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import scala.collection
import scala.collection.immutable._
import scala.collection.mutable.ListBuffer

import org.apache.spark.mllib.linalg.{Vector, Vectors, Matrices}
import org.apache.spark.mllib.linalg.distributed.RowMatrix

object myKeywords{
  def main(args: Array[String]) = {
    val spConfig = (new SparkConf).setAppName("myKeywords")// .setMaster("spark://s-es:7077")
    val sc = new SparkContext(spConfig)
    val kwLines = sc.textFile("/user/cloudera/keyword_matrix.txt")
    val contentLines = sc.textFile("/user/cloudera/content/*")
    //kwLines.collect.zipWithIndex.map(s => (s._1))
    val kwArray = kwLines.map(_.split(' ')(0)).collect
    // Array[String] = Array(柴油, 中油公司, 行政院, facebook, 臉書, 年輕世代, 便衣警察, 員警, 中正一分局, Skywatch,
    // 治本, 症狀, 大腸癌, 小松鼠, 康茄舞, 桃園國際機場, 出包, 油切, 放心, 烹煮無油)
    val kwMap = kwArray.zipWithIndex.toMap
    // scala.collection.immutable.Map[String,Int] = Map(烹煮無油 -> 19, 小松鼠 -> 13, 年輕世代 -> 5, 行政院 -> 2,
    // 桃園國際機場 -> 15, 大腸癌 -> 12, 放心 -> 18, 中正一分局 -> 8, Skywatch -> 9, 中油公司 -> 1, 柴油 -> 0, 油切 -> 17,
    // facebook -> 3, 症狀 -> 11, 康茄舞 -> 14, 治本 -> 10, 臉書 -> 4, 便衣警察 -> 6, 出包 -> 16, 員警 -> 7)
    // var kwSortedMap = SortedMap(kwMap.toArray:_*)   // 好像根本不需要sort @@
    val kwSwap = kwMap.map(s =>s.swap)
    // arraySortedMap: scala.collection.immutable.SortedMap[String,Int] = Map(Texas -> 2,
    // 中山醫學大學 -> 8, 國賠 -> 7, 工地 -> 4, 民雄車禍 -> 6, 緊急避孕藥 -> 5, 車險 -> 3, 集美 -> 1, 預防針 -> 9, ﻿萬海 -> 0)
    // println("We have " + kwSortedMap.size + " keywords")

    // this is no longer accurate, but it's what I originally wanted to do
    /*
    我要先做出一個array, 裡面放著這個keyword遇到其他keyword的次數
    然後再把這個array轉成sparse vector
    然後再放到 https://medium.com/@eob/how-you-might-create-a-scala-matrix-library-in-a-functional-programming-style-760f8bf6ee6
    的matrix裡面
     */

    def getNumIndexFromKW(kw: String): Int = if (kwMap.contains(kw)) kwMap(kw) else -1

    def getKWFromNumIndex(i: Int): String = if (kwSwap.contains(i)) kwSwap(i) else "oops"

    def getMatrixSideLength: Int = kwMap.size

    def containsBothKW(line:String, kw1:String, kw2:String):Boolean={
      if (line.contains(kw1) && line.contains(kw2)) true else false
    }

    def contentWalkThree(s:String):ListBuffer[(Int, Int)] = {
      var tt2 = ListBuffer[Tuple2[Int,Int]]()
      for (i <- kwArray){
        for (j <- kwArray){   // in the name of performance
          if (i != j ){
            if (containsBothKW(s,i,j)){
              tt2 += ((getNumIndexFromKW(i),getNumIndexFromKW(j)))  // scala likes its parenthesisessessss
            }
          }
        }
      }
      tt2
    }

    val coords_array_rdd3 = contentLines.map(s => contentWalkThree(s)).filter(_.size!=0)

    val coords3 = coords_array_rdd3.fold(ListBuffer[(Int,Int)]())(_++=_)

    val coordsG4 = sc.parallelize(coords3).groupByKey.map(s => (s._1, s._2.groupBy(s => s).map(s => (s._1, s._2.size) ))).cache()
    /*
    returns an Array[Int] of keyword indices
     */
    def getFriendlyKWIndexArray(kwIndex:Int) :Array[Int]= {
      coordsG4.filter(s => s._1 == kwIndex).map(s => s._2).map(s => s.keys).collect.flatten
    }
    /*
    returns an Array[Double] of keyword appearance counts
     */
    def getFriendlyKWTimesArray(kwIndex:Int) :Array[Int]= {
      coordsG4.filter(s => s._1 == kwIndex).map(s => s._2).map(s => s.values).collect.flatten
    }

    /*
    returns the number(count) of keywords that has accompanied a specific KEYWORD.  -1 if no such keyword
    usage: getColumnSum(svrdd, KEYWORD)
    */
    def getColumnSum(kw:String) : Int = {
      val kwIndex = getNumIndexFromKW(kw)
      if (kwIndex >= 0) getFriendlyKWTimesArray(kwIndex).size else -1
    }

    /*
    returns a list of all keywords that appeared in same news of KEYWORD, sorted by times, descending
     */
    def getFriendlyKW(kw:String):Array[(String,Int)] = {
      val kwIndex = getNumIndexFromKW(kw)
      if (kwIndex >= 0){
        val kwWordsArray = getFriendlyKWIndexArray(kwIndex).map(s=>getKWFromNumIndex(s))
        val kwTimesArray = getFriendlyKWTimesArray(kwIndex)
        val t = kwTimesArray zip kwWordsArray
        t.sortBy(x=>(x._1,x._2)).map(s=>s.swap)  // hope this works
      }else{
        println("keyword doesn't exist yo")
        Array[(String,Int)]()
      }
    }
    //println(getFriendlyKW("Skywatch").foreach(println))
    //kwArray.map(getFriendlyKW).foreach(_.foreach(println))
    for (a <- kwArray){
      println("keyword is: " + a)
      println("count of keywords that has appeared together with " + a + ": " + getColumnSum(a))
      println("list of keywords that has appeared together with " + a + ":")
      getFriendlyKW(a).foreach(println)
    }

    /*
    val fos = new FileOutputStream("/home/cloudera/myKeywords_coordsG4.obj")
    val oos = new ObjectOutputStream(fos)
    oos.writeObject(coordsG4)
    oos.close
    */
  } // end main()
}

/*
// ========================== TEN HOURS OF WORK BELOW ======================== GOODBYE ===========================
def contentWalkTwo(s:String):ListBuffer[(Int, Int)] = {
  var tt2 = ListBuffer[Tuple2[Int,Int]]()
  for (i <- kwArray){
    for (j <- kwArray.takeRight(kwArray.length - kwArray.indexOf(i) - 1)){
      if (i != j ){
        if (containsBothKW(s,i,j)){
          tt2 += ((getNumIndexFromKW(i),getNumIndexFromKW(j)))  // scala likes its parenthesisessessss
        }
      }
    }
  }
  tt2
}
val coords_array_rdd = contentLines.map(s => contentWalkTwo(s))// .filter(_.size!=0)
/* This is the intermediate result */
// Array[scala.collection.mutable.ListBuffer[(Int, Int)]] = Array(ListBuffer((0,1)),
// ListBuffer((2,3), (2,4), (2,5), (3,4), (3,5), (4,5)), ListBuffer(),
// ListBuffer((2,6), (2,7), (2,8), (2,9), (6,7), (6,8), (6,9), (7,8), (7,9), (8,9)),
// ListBuffer(), ListBuffer((11,12)), ListBuffer(), ListBuffer((13,14)), ListBuffer((15,16)),
// ListBuffer((17,18), (17,19), (18,19)))
// I have coordinates, how to turn them into a row for each kw?
val coords = coords_array_rdd.fold(ListBuffer[(Int,Int)]())(_++=_)
val coordsG = sc.parallelize(coords).groupByKey
// org.apache.spark.rdd.RDD[(Int, Iterable[Int])] = ShuffledRDD[98]
// coordsG: Array[(Int, Iterable[Int])] = Array((4,CompactBuffer(5)), (0,CompactBuffer(1)), (8,CompactBuffer(9)),
// (13,CompactBuffer(14)), (17,CompactBuffer(18, 19)), (6,CompactBuffer(7, 8, 9)), (18,CompactBuffer(19)),
// (2,CompactBuffer(3, 4, 5, 6, 7, 8, 9)), (15,CompactBuffer(16)), (11,CompactBuffer(12)), (3,CompactBuffer(4, 5)),
// (7,CompactBuffer(8, 9)))

// val mySV = coordsG.map(s => Vectors.sparse(s._2.size, (s._2 zip Stream.continually(1.0)).toSeq)) // does not have index
// val svrdd = coordsG.map(s => Vectors.sparse(s._2.size, ((Seq(s._1) ++ s._2) zip Stream.continually(1.0)).toSeq)) // has index(wrong index, don't use)
// val svrdd = coordsG.map(s => Vectors.sparse(getMatrixSideLength, ((Seq(s._1) ++ s._2) zip Stream.continually(1.0)).toSeq)) // 4 HOURS!!!(don't use)
// val rmat = new RowMatrix(mySVectorRDD)  // dunno how to use it

// =========== V2, two words could appear together in more than one news ===============
// coordsG.collect   org.apache.spark.rdd.RDD[(Int, Iterable[Int])] = ShuffledRDD
// Array[(Int, Iterable[Int])] = Array((4,CompactBuffer(5, 5)), (0,CompactBuffer(1, 1)), (8,CompactBuffer(9, 9)),
// (13,CompactBuffer(14, 14)), (17,CompactBuffer(18, 19, 18, 19)), (6,CompactBuffer(7, 8, 9, 7, 8, 9)),
// (18,CompactBuffer(19, 19)), (2,CompactBuffer(3, 4, 5, 6, 7, 8, 9, 3, 4, 5, 6, 7, 8, 9)), (15,CompactBuffer(16, 16)),
// (11,CompactBuffer(12, 12)), (3,CompactBuffer(4, 5, 4, 5)), (7,CompactBuffer(8, 9, 8, 9)))
val coordsG2 = coordsG.map(s => (s._1, s._2.groupBy(s => s).map(s => (s._1, s._2.size.toDouble) )))
// org.apache.spark.rdd.RDD[(Int, scala.collection.immutable.Map[Int,Double])] = MappedRDD
//                                                                   KW4 看見KW5 兩次
// Array[(Int, scala.collection.immutable.Map[Int,Double])] = Array((4,Map(5 -> 2.0)), (0,Map(1 -> 2.0)),
// (8,Map(9 -> 2.0)), (13,Map(14 -> 2.0)), (17,Map(19 -> 2.0, 18 -> 2.0)), (6,Map(8 -> 2.0, 7 -> 2.0, 9 -> 2.0)),
// (18,Map(19 -> 2.0)), (2,Map(5 -> 2.0, 6 -> 2.0, 9 -> 2.0, 7 -> 2.0, 3 -> 2.0, 8 -> 2.0, 4 -> 2.0)),
// (15,Map(16 -> 2.0)), (11,Map(12 -> 2.0)), (3,Map(5 -> 2.0, 4 -> 2.0)), (7,Map(8 -> 2.0, 9 -> 2.0)))
//val svrdd2 = coordsG2.map(s => Vectors.sparse(getMatrixSideLength, s._2.keys.toArray, s._2.values.toArray)) // no index

val svrdd = coordsG2.map(s => Vectors.sparse(getMatrixSideLength+1, Array(0)++s._2.keys, Array(s._1.toDouble)++s._2.values)) // has index(correct)
// SparseVector(size: Int, indices: Array[Int], values: Array[Double])
//            總keyword數量  有哪些keyword跟它一起出現            出現幾次
// Array[org.apache.spark.mllib.linalg.Vector] = Array(
// (20,[0,5],[4.0,2.0]),
// (20,[0,1],[0.0,2.0]),
// (20,[0,9],[8.0,2.0]),
// (20,[0,14],[13.0,2.0]),
// (20,[0,19,18],[17.0,2.0,2.0]),
// (20,[0,8,7,9],[6.0,2.0,2.0,2.0]),
// (20,[0,19],[18.0,2.0]),
// (20,[0,5,6,9,7,3,8,4],[2.0,2.0,2.0,2.0,2.0,2.0,2.0,2.0]),
// (20,[0,16],[15.0,2.0]),
// (20,[0,12],[11.0,2.0]),
// (20,[0,5,4],[3.0,2.0,2.0]),
// (20,[0,8,9],[7.0,2.0,2.0]))

val a = Vectors.sparse(5,Array(0,3,4),Array(2.0,2.0,1.0))
val b = Vectors.sparse(5,Array(1,2,4),Array(2.0,2.0,2.0))
val c = Vectors.sparse(5,Array(2,3,4),Array(3.0,2.0,2.0))
val d = Vectors.sparse(5,Array(0,1,4),Array(2.0,5.0,2.0))
val e = Vectors.sparse(5,Array(0,2,4),Array(2.0,2.0,7.0))
val f = sc.parallelize(Array(a,b,c,d,e))



// Array[Iterable[Int]] = Array(Set(5), Set(1), Set(9), Set(14), Set(19, 18), Set(8, 7, 9), Set(19),
// Set(5, 6, 9, 7, 3, 8, 4), Set(16), Set(12), Set(5, 4), Set(8, 9))


/*
returns the number of times that the two KEYWORDS have appeared together
 */
//    def getTimes(kw1:String, kw2:String):Int={

//  }


// ========================== SIX HOURS OF WORK BELOW ======================== GOODBYE ===========================

// actually i suspect this part will make it lose coherence
// i need a function for creating my sparse vector
val leftKwIndex = coords_array_rdd.map(s => s.split(',')(0)).map(_.toInt)
val rightKwIndex = coords_array_rdd.map(s => s.split(',')(1)).map(_.toInt)
// (0,1)(2,3)(2,4)(2,5)(3,4)(3,5)(4,5)(2,6)(2,7)(2,8)(2,9)(6,7)(6,8)(6,9)(7,8)(7,9)(8,9)(11,12)
// (13,14)(15,16)(17,18)(17,19)(18,19)
val coords = leftKwIndex zip rightKwIndex
val kwSortedMapSwap = kwSortedMap.map(_.swap)
val coordsG = coords.groupByKey
val sizeEach = coordsG.mapValues(s => s.size)
coordsG
// Array[(Int, Iterable[Int])] = Array((4,CompactBuffer(5)), (0,CompactBuffer(1)), (6,CompactBuffer(7, 8, 9)),
// (8,CompactBuffer(9)), (18,CompactBuffer(19)), (2,CompactBuffer(3, 4, 5, 6, 7, 8, 9)), (13,CompactBuffer(14)),
// (15,CompactBuffer(16)), (11,CompactBuffer(12)), (17,CompactBuffer(18, 19)), (3,CompactBuffer(4, 5)), (7,CompactBuffer(8, 9)))

coordsG.map(s=> s._2)
// Array[Iterable[Int]] = Array(CompactBuffer(5), CompactBuffer(1), CompactBuffer(7, 8, 9), CompactBuffer(9),
// CompactBuffer(19), CompactBuffer(3, 4, 5, 6, 7, 8, 9), CompactBuffer(14), CompactBuffer(16), CompactBuffer(12),
// CompactBuffer(18, 19), CompactBuffer(4, 5), CompactBuffer(8, 9))

coordsG.map(s=> s._2).map(s => s zip Stream.continually(1))
// Array[Iterable[(Int, Int)]] = Array(List((5,1)), List((1,1)), List((7,1), (8,1), (9,1)), List((9,1)),
// List((19,1)), List((3,1), (4,1), (5,1), (6,1), (7,1), (8,1), (9,1)), List((14,1)), List((16,1)), List((12,1)),
// List((18,1), (19,1)), List((4,1), (5,1)), List((8,1), (9,1)))

// the 0-th element of my sparse vector will be its index, elements from 1 onwards are the keywords that it is
// associated with (i.e. appeared together)

coords_array_rdd
// Array[String] = Array(0,1, 2,3, 2,4, 2,5, 3,4, 3,5, 4,5, 2,6, 2,7, 2,8, 2,9, 6,7, 6,8, 6,9, 7,8, 7,9,
// 8,9, 11,12, 13,14, 15,16, 17,18, 17,19, 18,19)

// addInMatrix("中油公司","康茄舞")
def addInMatrix(i:String, j:String): Unit ={
  println("we need to add 1 to the coordinate (" + getNumIndexFromKW(i) + "," + getNumIndexFromKW(j) + ")")
  //tempList += "(" + getNumIndexFromKW(i) + "," + getNumIndexFromKW(j) + ")"
}

def returnOne(length:Int) :Array[Double]= {
  var seqq = Array[Double]()
  for (i <- 0 until length){
    seqq ++= Array(1.0)
  }
  seqq
}
@deprecated
def contentWalkOne(s:String):ArrayBuffer[String] = {
  var tempList = ArrayBuffer[String]()
  for (i <- kwArray){
    for (j <- kwArray.takeRight(kwArray.length - kwArray.indexOf(i) - 1)){
      if (i != j ){
        if (containsBothKW(s,i,j)){
          //addInMatrix(i,j)
          //return getNumIndexFromKW(i) + "," + getNumIndexFromKW(j)
          tempList += getNumIndexFromKW(i) + "," + getNumIndexFromKW(j)
        }
      }
    }
  }
  tempList
}

// ========================== FOUR HOURS OF WORK BELOW ======================== GOODBYE =======================
val a = Array(1,2,3,4)
val b = Array(1,2,3,4)
val c = Array(1,2,3,4)
val l = Array((1,a),(2,b),(3,c))
//l: Array[(Int, Array[Int])] = Array((1,Array(1, 2, 3, 4)), (2,Array(1, 2, 3, 4)), (3,Array(1, 2, 3, 4)))
val pl = sc.parallelize(l)
//pl: org.apache.spark.rdd.RDD[(Int, Array[Int])] = ParallelCollectionRDD[148] at parallelize at <console>:35
val pp = pl.map(s => Vectors.sparse(s._2.size,((Seq(s._1)++s._2) zip Stream.continually(1.0)).toSeq))
val ppp = pl.map(s => Vectors.sparse(s._2.size,(Seq(s._1)++s._2).toArray,returnOne(s._2)))
//pp: org.apache.spark.rdd.RDD[org.apache.spark.mllib.linalg.Vector] = MappedRDD[153] at map at <console>:39

//val pp = pl.map(s => Vectors.sparse(s._2.size,((Seq(s._1)++s._2) zip Stream.continually(1.0)).toSeq) )
val pp = for {
  coordsG
}
//val srTest = sc.parallelize(List(sv1,sv2))
*/





