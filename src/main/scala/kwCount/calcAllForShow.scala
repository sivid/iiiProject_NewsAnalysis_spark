package kwCount

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * Created by sivid on 10/28/14.
 * outputs a (keyword:String, appeared_in_number_of_documents:Int) list
 */
object calcAllForShow {
  val spConfig = (new SparkConf).setAppName("calcAll")
  val sc = new SparkContext(spConfig)
  // val contentLines = sc.textFile("/user/cloudera/2013-2014/*", 12)
  val contentLines = sc.textFile("/user/cloudera/SampleContent.txt", 12)
  val kwLines = sc.textFile("/user/cloudera/WordDic.txt")
  val kwArray = kwLines.map(_.split(' ')(0)).collect  // spark doesn't allow rdd-in-rdd loops
  val kwCount = contentLines.flatMap{ a =>
    kwArray.map{ b =>
      if (a.contains(b)) (b,1)
      else (b,0)
    }
  }.filter(_._2!=0).groupBy(_._1).map(s => (s._1, s._2.size)).map(
        _.swap).sortByKey().coalesce(1).saveAsTextFile("/user/cloudera/qq1")
}
