import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.functions._
import com.databricks.spark.corenlp.functions._

val rawtextDF = finalRDD.toDF("username", "raw_text")

val cleanDF = rawtextDF.select('username, cleanxml('raw_text).as('text)).filter($"text" =!= "")
val lemmasDF = cleanDF.select('username, lemma('text).as('lemmas) )// .select('id, explode(ssplit('doc)).as('sen)).....tokenize('sen).as('words)
lemmasDF.show(5)

// val regexTokenizer = new RegexTokenizer().setInputCol("lemmas").setOutputCol("raw_words").setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)
// val wordsData = regexTokenizer.transform(sentenceData)

val remover = new StopWordsRemover().setInputCol("lemmas").setOutputCol("words")
val wordsFiltered = remover.transform(lemmasDF)
wordsFiltered.show(5)

// fit a CountVectorizerModel from the corpus
val cvModel: CountVectorizerModel = new CountVectorizer().setInputCol("words").setOutputCol("rawFeatures").fit(wordsFiltered)
cvModel.vocabulary
val featurizedData = cvModel.transform(wordsFiltered)
featurizedData.show(5)

val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
val idfModel = idf.fit(featurizedData)

val rescaledData = idfModel.transform(featurizedData)
rescaledData.select("username", "features").show(5)

val tfidf_dataframe = rescaledData
