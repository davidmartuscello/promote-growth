//spark2-shell --jars stanford-corenlp-3.9.1-models.jar,spark-corenlp-0.4.0-spark2.4-scala2.11.jar,stanford-corenlp-3.9.1.jar

import org.apache.spark.sql.SparkSession

val spark: SparkSession = SparkSession.builder.master("local").getOrCreate

// input file path
//val path = "project/output/part-16086-f23e65b5-2efe-4ea6-b3ba-4a0b703242f8-c000.json"
val path = "hdfs:///user/ttc290/project/output/*"

// load dataset
//val df = spark.read.json("project/output/*")
val df = spark.read.json(path)
val new_df = df.drop("score_hidden")

import com.databricks.spark.corenlp.functions._
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.clustering.LDA

// combine all comments of each user into one string
val sentenceData = new_df.groupBy("author").agg(concat_ws(". ", collect_list("body")) as "posts")

// keep author and post columns
val rawtextDF2 = sentenceData.toDF("author", "posts")
val rawtextDF = rawtextDF2.filter(length($"posts") > 0)

// load proposal
val proposalLines = sc.textFile("hdfs:///user/ttc290/project/Proposal_Example_Questions.txt")
val proposalString = proposalLines.reduce( (x,y) => x+y )
val proposalKey = sc.parallelize( Seq( ("[deleted]", proposalString) ) ).toDF("author", "posts")

// join proposal to dataframe
val appendedDF = rawtextDF.union(proposalKey)

// split user's combined comments into words
val regexTokenizer = new RegexTokenizer().setInputCol("posts").setOutputCol("raw_words").setPattern("\\W")

// val wordsData = tokenizer.transform(sentenceData)
val wordsData = regexTokenizer.transform(appendedDF)

// remove stop words
val remover = new StopWordsRemover().setInputCol("raw_words").setOutputCol("words")

val wordsFiltered = remover.transform(wordsData)
//wordsFiltered.show()

// fit a CountVectorizerModel from the corpus
// only include words that appear in at least 5 comments
// this is the TF vector (feature vector)
val cvModel: CountVectorizerModel = new CountVectorizer().setInputCol("words").setOutputCol("rawFeatures").setMinDF(5).fit(wordsFiltered)
//cvModel.vocabulary

val featurizedData = cvModel.transform(wordsFiltered)
//featurizedData.show()

// create IDF vector
val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
val idfModel = idf.fit(featurizedData)

// create TFIDF vector for each user 
val tfidf_dataframe = idfModel.transform(featurizedData)
//tfidf_dataframe.show(5)

//LDA
val dataset = tfidf_dataframe.select("author", "features")

// 160 topics, 10 iterations
val lda = new LDA().setK(160).setMaxIter(10)
val model = lda.fit(dataset)

//val ll = model.logLikelihood(dataset)
//val lp = model.logPerplexity(dataset)

//println(s"The lower bound on the log likelihood of the entire corpus: $ll")
//println(s"The upper bound on perplexity: $lp")

// use the top 10 words to describe a topic
val topics = model.describeTopics(10)
println("The topics described by their top-weighted terms:")
//topics.show()

val transformed = model.transform(dataset)
//transformed.show()

import org.apache.spark.sql.functions.udf
import scala.collection.mutable.WrappedArray

val vocab = spark.sparkContext.broadcast(cvModel.vocabulary)
val toWords = udf( (x : WrappedArray[Int]) => { x.map(i => vocab.value(i)) })

// use the top 10 words to describe a topic
val topics = model.describeTopics(10).withColumn("topicWords", toWords(col("termIndices")))
//topics.select("topic", "topicWords").show(false)

val wordsWithWeights = udf( (x : WrappedArray[Int],
                             y : WrappedArray[Double]) => 
    { x.map(i => vocab.value(i)).zip(y)}
)

// use the top 10 words to describe a topic
val topics2 = model.describeTopics(10).withColumn("topicWords", 
      wordsWithWeights(col("termIndices"), col("termWeights"))
)
//topics2.select("topic", "topicWords").show(false)

val topics2exploded = topics2.select("topic", "topicWords").withColumn("topicWords", explode(col("topicWords")))
topics2exploded.show(false)

val finalTopic = topics2exploded.select(
    col("topic"), 
    col("topicWords").getField("_1").as("word"), 
    col("topicWords").getField("_2").as("weight")
  )
//finalTopic.show(5, false)

// Compute Cosine similarity score between every row and author "[deleted]" which is the proposal
import org.apache.spark.sql.Row
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}

val tfidf = transformed.select("author", "topicDistribution")

// create an RDD
val testrdd = tfidf.rdd.map {case Row(x, v: org.apache.spark.ml.linalg.Vector) => (x, org.apache.spark.mllib.linalg.Vectors.fromML(v))}

// get the proposal vector as target
val targetrdd = testrdd.filter(x => x._1 == "[deleted]")
val targetvec = targetrdd.take(1)(0)._2

// similarity function
def similarity(u: Vector, p: Vector): Double = {
  val u_breeze = new BDV(u.toArray)
  val p_breeze = new BDV(p.toArray)
  val dot_product = u_breeze dot p_breeze
  dot_product/(Vectors.norm(u,2)*Vectors.norm(p,2))
}

// create a pair RDD (author, similarity score)
val resultrdd = testrdd.map(x => (x._1, similarity(x._2, targetvec)))

//Display Ranked List of Most Similar Users
val sortedUsers = resultrdd.sortBy(_._2, false).toDF("Username", "SimilarityScore")

// filter NaN score
val sortedUserList = sortedUsers.filter(!isnan($"SimilarityScore"))

// show top 15 users
sortedUserList.show(15)
