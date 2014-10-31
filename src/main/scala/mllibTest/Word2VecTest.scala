package mllibTest

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.feature.Word2Vec
import java.io.FileOutputStream
import java.io.ObjectOutputStream

/**
 * Created by sivid on 10/21/14.
http://apache-spark-user-list.1001560.n3.nabble.com/How-to-save-mllib-model-to-hdfs-and-reload-it-td11953.html
To save the model:
import java.io.FileOutputStream
import java.io.ObjectOutputStream
val fos = new FileOutputStream("/root/kw_count_spark/models/w2vmodel.obj")
val oos = new ObjectOutputStream(fos)
oos.writeObject(model)
oos.close

and load it in:
import java.io.FileInputStream
import java.io.ObjectInputStream
val fis = new FileInputStream("/root/kw_count_spark/models/w2vmodel.obj")
val ois = new ObjectInputStream(fis)
val model = ois.readObject().asInstanceOf[org.apache.spark.mllib.feature.Word2VecModel]
model.findSynonyms("黨員", 10)
 spark-submit --class mllibTest.Word2VecTest thematrix_2.10-1.0.jar
 */
object Word2VecTest extends App{

  val spConfig = (new SparkConf).setAppName("myKeywords")// .set("spark.executor.memory", "1g")
  val sc = new SparkContext(spConfig)

  // val input = sc.textFile("/mllib/resultbigTrad.txt").map(line => line.split(" ").toSeq)  // created using ansj
  val input = sc.textFile("/word2vec/newscontent_jieba.txt").map(line => line.split(" ").toSeq) // created using jieba

  val word2vec = new Word2Vec()
  val model = word2vec.fit(input)

  //val fos = new FileOutputStream("/root/kw_count_spark/models/w2vmodel_jieba.obj")
  //val fos = new FileOutputStream("/root/w2vmodel_jieba.obj")
  //val oos = new ObjectOutputStream(fos)
  //oos.writeObject(model)
  //oos.close

  val synonyms = model.findSynonyms("憲法", 10)
  // model.findSynonyms("黨員", 10)
  // model.findSynonyms("紅色", 10)
  // model.findSynonyms("香港", 10)
  // model.findSynonyms("特首", 10)
  // model.findSynonyms("民進黨", 10)
  // model.findSynonyms("國民黨", 10)
  // model.findSynonyms("佔中", 10)  // not in vocabulary

  for((synonym, cosineSimilarity) <- synonyms) {
    println(s"$synonym $cosineSimilarity")
  }


}
