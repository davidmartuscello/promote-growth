import org.apache.spark.sql.Row
import org.apache.spark.mllib.linalg.{Vector, Vectors, DenseVector}
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}

def similarity(u: Vector, p: Vector): Double = {
  val u_breeze = new BDV(u.toArray)
  val p_breeze = new BDV(p.toArray)
  val dot_product = u_breeze dot p_breeze
  dot_product/(Vectors.norm(u,2)*Vectors.norm(p,2))
}

//Select TFIDF Vectors
val tfidf = tfidf_dataframe.select("username", "features")
val vectorRDD = tfidf.rdd.map {case Row(x, v: org.apache.spark.ml.linalg.Vector) => (x.toString,org.apache.spark.mllib.linalg.Vectors.fromML(v))}

//Separate Proposal Vector from User Vectors
val pRDD = vectorRDD.filter(x => x._1 == "PROPOSAL")
val p = pRDD.take(1)(0)._2

//Compute Individual Similarity Score by Username:
val uRDD = testrdd.filter(x => x._1 == "daniel")
val u = uRDD.take(1)(0)._2
similarity(u,p)

//Compute Similarity Between Users and Proposal
val resultsRDD = vectorRDD.map { case (u, v) => (u, similarity(v,p)) }

//Display Ranked List of Most Similar Users
resultsRDD.sortBy(_._2, false).toDF.show(5)
