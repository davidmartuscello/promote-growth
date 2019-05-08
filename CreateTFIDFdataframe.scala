import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.functions._
import com.databricks.spark.corenlp.functions._

val rawtextDF = finalRDD.toDF("username", "raw_text").filter($"raw_text" =!= "")

//BROKEN LEMMATIZATION FEATURE ----------------------
// val cleanDF = rawtextDF.select('username, cleanxml('raw_text).as('text))
// val rawlemmasDF = cleanDF.select('username, lemma('text).as('lemmas) )
// rawlemmasDF.show(5)
// val rawLemmasDF = cleanDF.select('username,'lemmas).map( (x: String ,y: Array[String] ) => (x,y) ).reduceByKey(a,b => a++b).toDF("username","lemmas")
// val cleanDF = toksDF.withColumn("toks", combSen($"toks"))
//-------------------

val textDF = rawtextDF.select('username, cleanxml('raw_text).as('text))//.select('username, tokenize('text).as('toks))

//Tokenize text
val regexTokenizer = new RegexTokenizer().setInputCol("text").setOutputCol("toks").setPattern("\\W+")
val toksDF = regexTokenizer.transform(textDF)

//Custom Function to remove punctuation and words less than length 3
val combSen = udf( (x:Seq[String]) => x.filterNot(_.matches("\\W+")).filterNot(_.length() < 3) )

//Remove Stopwords and Punctuation
val remover = new StopWordsRemover().setInputCol("toks").setOutputCol("words")
val wordsFiltered = remover.transform(toksDF).withColumn("words", combSen($"words")).withColumn("arrayLen", size($"words")).filter($"arrayLen" =!= 0).drop("arrayLen")//.withColumn("arrayLen", size($"words")).filter($"arrayLen" =!= 0).drop("arrayLen")//.filter($"words".length =!= 0)
wordsFiltered.show(5)

// fit a CountVectorizerModel from the corpus
val cvModel: CountVectorizerModel = new CountVectorizer().setInputCol("words").setOutputCol("rawFeatures").fit(wordsFiltered)
cvModel.vocabulary
val featurizedData = cvModel.transform(wordsFiltered)
featurizedData.show(5)

//Compute TFIDF Dataframe
val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
val idfModel = idf.fit(featurizedData)
val tfidf_dataframe = idfModel.transform(featurizedData)
tfidf_dataframe.select("username", "features").show(5)
