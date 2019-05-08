// LDA
import org.apache.spark.ml.clustering.LDA

val dataset = tfidf_dataframe.select("username", "features")
val lda = new LDA().setK(5).setMaxIter(10)
val model = lda.fit(dataset)

// Statistics on Dataset
val ll = model.logLikelihood(dataset)
val lp = model.logPerplexity(dataset)
println(s"The lower bound on the log likelihood of the entire corpus: $ll")
println(s"The upper bound on perplexity: $lp")

val topics = model.describeTopics(10)
println("The topics described by their top-weighted terms:")
topics.show()

val transformed = model.transform(dataset)
transformed.show()

import org.apache.spark.sql.functions.udf
import scala.collection.mutable.WrappedArray

val toWords = udf( (x : WrappedArray[Int]) => { x.map(i => cvModel.vocabulary(i)) })
val topics = model.describeTopics(10).withColumn("topicWords", toWords(col("termIndices")))
topics.select("topic", "topicWords").show(false)

val wordsWithWeights = udf( (x : WrappedArray[Int],
                             y : WrappedArray[Double]) =>
    { x.map(i => cvModel.vocabulary(i)).zip(y)}
)

val topics2 = model.describeTopics(10).withColumn("topicWords",
      wordsWithWeights(col("termIndices"), col("termWeights"))
)
topics2.select("topic", "topicWords").show(false)

val topics2exploded = topics2.select("topic", "topicWords").withColumn("topicWords", explode(col("topicWords")))
topics2exploded.show(false)

val finalTopic = topics2exploded.select(
    col("topic"),
    col("topicWords").getField("_1").as("word"),
    col("topicWords").getField("_2").as("weight")
  )
finalTopic.show
