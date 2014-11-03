package newsClassification

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import scala.collection.mutable.{ListBuffer, ArrayBuffer}
import scala.language.postfixOps
import java.io.FileInputStream
import java.io.ObjectInputStream
import java.io.FileOutputStream
import java.io.ObjectOutputStream


/**
 * Created by sivid on 10/24/14.
 * whole lot of credits goes to
 * http://chimpler.wordpress.com/2014/06/11/classifiying-documents-using-naive-bayes-on-apache-spark-mllib/
 * I tried to at least rewrite this program,
 * alas, skill and time limitations were too much to overcome.
 * In the end, the only code I could call my own was the chinese tokenization method.
spark-submit --class newsClassification.newsClassification thematrix_2.10-1.0.jar
 */
class Dictionary(dict: Seq[String]) extends Serializable{
  // map term => index
  val termToIndex = dict.zipWithIndex.toMap
  @transient
  lazy val indexToTerm = termToIndex.map(_.swap)
  val count = termToIndex.size
  def indexOf(term: String) = termToIndex(term)
  def valueOf(index: Int) = indexToTerm(index)
  def tfIdfs(terms: Seq[String], idfs: Map[String, Double]) = {
    val filteredTerms = terms.filter(idfs contains)
    (filteredTerms.groupBy(identity).map {
      case (term, instances) =>
        (indexOf(term), (instances.size.toDouble / filteredTerms.size.toDouble) * idfs(term))
    }).toSeq.sortBy(_._1) // sort by termId
  }
  def vectorize(tfIdfs: Iterable[(Int, Double)]) = {
    Vectors.sparse(dict.size, tfIdfs.toSeq)
  }
}

case class TermDoc(doc: String, label: String, terms: Seq[String])
// case class Document(docId: String, body: String = "", labels: Set[String] = Set.empty)
object Tokenizer extends Serializable{
  def tokenizeAll(docs: org.apache.spark.rdd.RDD[String], kwArray:Array[String]) = docs.map(s => tokenize(s, kwArray))
  def tokenize(line: String, kwArray: Array[String]): TermDoc = {
    TermDoc(line.split('\t')(0), line.split('\t')(1), stringToVec(line.split('\t')(2), kwArray))
  }

  //val testLine = "下午時分上權上壘上映上壘上映呵呵3erteer"
  //val testVector = Vector("下午","時分","上權","上壘","上壘上","壘上映", "上映")
  //val testVector = Vector("下午","時分","上權","上壘","壘上映", "上映")
  def stringToVec(line: String, kwArray: Array[String]): Seq[String] = {
    var stringDone = false
    var tempLine = line
    var result = Seq("")

    while(!stringDone){
      case class Candidate(name: String, place: Int)
      def leftMost(c1: Candidate, c2: Candidate): Candidate = if (c1.place < c2.place) c1 else c2
      var thisRoundContains = ArrayBuffer[Candidate]()
      for (kw <- kwArray){
        if (tempLine.contains(kw)){
          thisRoundContains += Candidate(kw, tempLine.indexOf(kw))
        }
      } // end for
      if (thisRoundContains.size != 0) {
        val leftMostWord = thisRoundContains.reduceLeft(leftMost)
        result = result :+ leftMostWord.name
        tempLine = tempLine.splitAt(leftMostWord.place + leftMostWord.name.size)._2
      }
      else{  // nothing more to take from this line
        stringDone = true
      }
    } // end while
    result.filter(_.size!=0)
  }
}

object newsClassification extends App {
  val spConfig = (new SparkConf).setAppName("newsClassification")
  // .set("spark.executor.memory", "1g")
  val sc = new SparkContext(spConfig)
  val trainingLines = sc.textFile("/user/cloudera/ETTV_training_indexed.txt", 12)
  val kwLines = sc.textFile("/user/cloudera/WordDic.txt")
  val kwArray = kwLines.map(_.split(' ')(0)).collect

  val termDocsRdd = Tokenizer.tokenizeAll(trainingLines, kwArray)
  val termDocs = termDocsRdd.collect
  val numDocs = termDocs.size

  // create dictionary term => id
  // and id => term
  val terms = termDocs.flatMap(_.terms).distinct.sortBy(identity)
  val termDict = new Dictionary(terms)

  // ====================================================================================

  val labels = termDocs.map(_.label).distinct
  val labelDict = new Dictionary(labels)

  // compute TFIDF and generate vectors
  // for IDF
  val idfs = termDocs.flatMap(termDoc => termDoc.terms.map((termDoc.doc, _))).groupBy(_._2).map {
    case (term, docs) => term -> (numDocs.toDouble / docs.size.toDouble)
  }
  // idfs: scala.collection.immutable.Map[String,Double] = Map(凍人 -> 4623.333333333333, 競爭激烈 -> 295.1063829787234,
  // 鮮人 -> 13870.0, 資本 -> 96.99300699300699, 住宅政策 -> 13870.0, 約有 -> 91.8543046357616, 台北報導 -> 25.828677839851025,

  val tfidfs = termDocsRdd map { termDoc =>
    val termPairs = termDict.tfIdfs(termDoc.terms, idfs)
    // we consider here that a document only belongs to the first label
    val labelId = labelDict.indexOf(termDoc.label).toDouble
    val vector = Vectors.sparse(termDict.count, termPairs)
    LabeledPoint(labelId, vector)
  }
  // tfidfs: org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint] = MappedRDD[10] at map at <console>:47

  val model = NaiveBayes.train(tfidfs)
  // org.apache.spark.mllib.classification.NaiveBayesModel
  val fos = new FileOutputStream("/home/cloudera/iii/naiveBayesModel.obj")
  val oos = new ObjectOutputStream(fos)
  oos.writeObject(termDict)
  oos.close
  println("Naive Bayes model done")
  // things we still need to use this model: kwArray, idfs, input files

  val fos1 = new FileOutputStream("/home/cloudera/iii/naiveBayes_idfs.obj")
  val oos1 = new ObjectOutputStream(fos1)
  oos1.writeObject(idfs)
  oos1.close
/*
scala>   oos.writeObject(termDict)
java.io.NotSerializableException: java.io.FileInputStream          // ..... wtf

val fis = new FileInputStream("/home/cloudera/iii/naiveBayesModel.obj")
val ois = new ObjectInputStream(fis)
val model = ois.readObject().asInstanceOf[org.apache.spark.mllib.classification.NaiveBayesModel]
val fis1 = new FileInputStream("/home/cloudera/iii/naiveBayes_idfs.obj")
val ois1 = new ObjectInputStream(fis1)
val idfs = ois1.readObject().asInstanceOf[scala.collection.immutable.Map[String,Double]]
val kwLines = sc.textFile("/user/cloudera/WordDic.txt")
val kwArray = kwLines.map(_.split(' ')(0)).collect
 */

  //   single line file
  val testDoc = sc.textFile("/user/cloudera/NB_input_test.txt")
  val testTerms = Tokenizer.stringToVec(testDoc.collect.apply(0), kwArray)
  val tfIdfs_testing = termDict.tfIdfs(testTerms, idfs)
  val vector_testing = termDict.vectorize(tfIdfs_testing)
  val labelId_testing = model.predict(vector_testing)
  println("Label: " + labelDict.valueOf(labelId_testing.toInt))

  // ======================================= use this, multiple line file =======================================
  // val testDocAll = sc.textFile("/user/cloudera/contentTest/contentShort.txt",12)
  // val testDocAll = sc.textFile("/user/cloudera/content/CNA.txt",12)
  // Array[(String, Int)] = Array((體育,447), (影劇,169), (政治,572), (財經,808), (國際,823), (3C,306), (生活,248), (地方,752), (社會,321), (新奇,838), (論壇,480), (大陸,401))
  // val testDocAll = sc.textFile("/user/cloudera/content/ChinaTime.txt",12)
  // Array[(String, Int)] = Array((政治,167), (地方,277), (社會,81), (影劇,156), (國際,217), (3C,170), (生活,102), (大陸,216), (體育,533), (財經,636), (論壇,200), (新奇,310))
  // val testDocAll = sc.textFile("/user/cloudera/content/NewTalk.txt",12)
  // result: Array[(String, Int)] = Array((政治,577), (地方,125), (社會,71), (影劇,13), (國際,220), (3C,41), (生活,45), (大陸,108), (體育,25), (財經,67), (論壇,487), (新奇,423))
  // val testDocAll = sc.textFile("/user/cloudera/content/nownews.txt",12)
  // (政治,4924), (地方,1974), (社會,603), (影劇,1128), (國際,2734), (3C,2805), (生活,1494), (大陸,1164), (體育,452), (財經,2927), (論壇,3606), (新奇,4435)
  val testDocAll = sc.textFile("/user/cloudera/content/Apple.txt",12)
  // (政治,18264), (地方,53988), (社會,28603), (影劇,73244), (國際,56463), (3C,38425), (生活,19811), (大陸,32189), (體育,61434), (財經,56397), (論壇,34123), (新奇,88951)
  val testTermsAll = testDocAll.map(s => Tokenizer.stringToVec(s, kwArray))
  val tfIdfsAll = testTermsAll.map(s => termDict.tfIdfs(s, idfs))   // why
  val vectorAll = tfIdfsAll.map(termDict.vectorize)
  val labelIdAll = vectorAll.map(model.predict)
  //  res7: Array[Double] = Array(9.0, 0.0, 9.0, 11.0, 8.0, 1.0, 7.0, 1.0, 4.0, 8.0)
  var labels_list = Seq[Tuple2[String,Int]]()
  val labels_list2 = labelIdAll.flatMap{s =>
    println("Label: " + labelDict.valueOf(s.toInt))
    labels_list :+ (labelDict.valueOf(s.toInt), 1)
  }
  val result = labels_list2.groupBy(_._1).map(s => (s._1, s._2.size)).collect





  //   files in a directory
  /*
  var tt2 = List[Tuple2[String,Int]]()
  var i = 0
  val testDocAll = sc.textFile("/user/cloudera/contentTest/contentShort.txt")
  for (line <- testDocAll){
    val testTerms = Tokenizer.stringToVec(line, kwArray)
    val tfIdfs = termDict.tfIdfs(testTerms, idfs)
    val vector = termDict.vectorize(tfIdfs)
    val labelId = model.predict(vector)
    tt2 = tt2 :+ (labelDict.valueOf(labelId.toInt),1)
  }
  */




}


  /*  my code... Q.Q
  val fis = new FileInputStream("/home/cloudera/iii/tokenized_collect.obj")
  val ois = new ObjectInputStream(fis)
  val tokenized_collect = ois.readObject().asInstanceOf[Array[Seq[String]]]
  ois.close
  val tokens = sc.parallelize(tokenized_collect, 12)

  val terms = tokenized_collect.flatMap(s => s).distinct.sortBy(identity)
  // val termDict = new Dictionary(terms)
  val labels = trainingLines.map(_.split('\t')(1)).distinct
  //  idf = (number of documents) / (number of documents containing term)
  // almost directly copied from my calcAll code :p
  val idfRdd = tokenized_collect.flatMap{ a =>
    kwArray.map{ b =>
      if (a.contains(b)) (b,1)
      else (b,0)
    }
  }.filter(_._2!=0).groupBy(_._1).map(s => (s._1, s._2.size.toDouble / trainingLines.count.toDouble))
  //val idfRdd_collect = idfRdd.collect
  val fos = new FileOutputStream("/home/cloudera/iii/idf.obj")
  val oos = new ObjectOutputStream(fos)
  oos.writeObject(idfRdd)
  oos.close
  println("idfRdd done")
  */

  /* http://spark.apache.org/docs/1.1.0/mllib-feature-extraction.html#tf-idf
  val hashingTF = new HashingTF()
  val tf = hashingTF.transform(tokenized)
  tf.cache()
  val idf = new IDF().fit(tf)
  val tfidf = idf.transform(tf)
  */


  /*  okay, after wasting much time on recursion, I've decided to use a while().
  containsAnyInArray("多夫多多陳喬年多")
  containsAnyInArray("sdfsdfs")
  val testline = "下午時分上權上壘上壘上壘上壘上映"
  val kwVector = Vector("下午","時分","上權","上壘","上壘上","壘上映")
  substringToSeq(testline, kwVector)
  def substringToSeq(line: String, kwVector:Vector[String]):Vector[String] = {
    @tailrec def containsAnyInArray(line:String, result: Vector[String]) : Vector[String] = {
      var idx=0
      var tempResult=Vector("")
      var trueEnd = false
      println("5, line: " + line)
      for (kw <- kwVector){
        breakable{
          if (line.contains(kw)){
            idx = line.indexOf(kw) + kw.size
            tempResult = result ++ Vector(kw)
            println("1: kw: " + kw + ", line: " + line)
            break
          }
          else {
            println("2: kw: " + kw + ", line: " + line)
          }
          println("@@@")
          trueEnd = true      // there are no further keywords in this line
        } // end breakable
      } // end for loop
      println("3: " + line)
      if (trueEnd) {
        println("end")
        result
      }
      else {
        println("re")
        containsAnyInArray(line.splitAt(idx)._2, tempResult)
      }
    } // end tailrec
    println("4: " + line)
    containsAnyInArray(line, Vector(""))
  }

  def countSubstring(str1:String, str2:String):Int={
    @tailrec def count(pos:Int, c:Int):Int={
      val idx=str1 indexOf(str2, pos)
      if(idx == -1) c else count(idx+str2.size, c+1)
    }
    count(0,0)
  }
  */
