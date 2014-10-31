package m1

/**
 * Created by sivid on 10/15/14.
 * $HADOOP_HOME/sbin/start-dfs.sh;$HADOOP_HOME/sbin/start-yarn.sh;$SPARK_HOME/sbin/start-all.sh
 * http://btsxj.wordpress.com/2014/08/17/multiplication-of-matrices-on-spark-mllib/
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._

import org.apache.spark.mllib.linalg.{Vector, Vectors, Matrices}
import org.apache.spark.mllib.linalg.distributed.RowMatrix

object LATest extends App {

  val spConfig = (new SparkConf).setMaster("local").setAppName("SparkLA")
  val sc = new SparkContext(spConfig)

  // Load and parse the data file.
  // 0 -1 2
  // 4 11 2
  // res2: Array[org.apache.spark.mllib.linalg.Vector] = Array((3,[1,2],[-1.0,2.0]), (3,[0,1,2],[4.0,11.0,2.0]))
  val rows = sc.textFile("/matrixTest/latest3.txt").map { line =>
    val values = line.split(' ').map(_.toDouble)
    Vectors.sparse(values.length, values.zipWithIndex.map(e => (e._2, e._1)).filter(_._2 != 0.0))
    //  SparseVector(size: Int, indices: Array[Int], values: Array[Double])
  }
  // Build a distributed RowMatrix
  val rmat = new RowMatrix(rows)

  // Build a local DenseMatrix
  // 3 -1
  // 1 2
  // 6 1
  // res3: Array[org.apache.spark.mllib.linalg.Vector] = Array([3.0,-1.0], [1.0,2.0], [6.0,1.0])
  val dm = sc.textFile("/matrixTest/latest4.txt").map { line =>
    val values = line.split(' ').map(_.toDouble)
    Vectors.dense(values)
  }

  val ma = dm.map(_.toArray).take(dm.count.toInt)
  val localMat = Matrices.dense( dm.count.toInt, dm.take(1)(0).size, transpose(ma).flatten)

  // Multiply two matrices
  rmat.multiply(localMat).rows.foreach(println)

  /**
   * Transpose a matrix
   */
  def transpose(m: Array[Array[Double]]): Array[Array[Double]] = {
    (for {
      c <- m(0).indices
    } yield m.map(_(c)) ).toArray
  }
}
