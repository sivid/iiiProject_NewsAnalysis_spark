package m2

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

import scala.collection.mutable.ListBuffer

/**
 * Created by sivid on 10/28/14.
 */
object myKeywordsForShow {
  def main(args: Array[String]) = {
    val spConfig = (new SparkConf).setAppName("myKeywords")// .setMaster("spark://s-es:7077")
    val sc = new SparkContext(spConfig)
    val kwLines = sc.textFile("/user/cloudera/keyword_matrix.txt")
    val contentLines = sc.textFile("/user/cloudera/content/*")
    //kwLines.collect.zipWithIndex.map(s => (s._1))
    val kwArray = kwLines.map(_.split(' ')(0)).collect
    val kwMap = kwArray.zipWithIndex.toMap
    val kwSwap = kwMap.map(s =>s.swap)

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
