import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.functions._
import com.databricks.spark.corenlp.functions._

val rawtextDF = finalRDD.toDF("username", "raw_text").filter($"raw_text" =!= "")

//BROKEN LEMMATIZATION FEATURE
// val cleanDF = rawtextDF.select('username, cleanxml('raw_text).as('text)).select('username, tokenize('text).as('toks)).withColumn("toks", combSen($"toks")).withColumn("arrayLen", size($"toks")).filter($"arrayLen" =!= 0).drop("arrayLen")
// val makeSTRING = udf((x:Seq[String]) => x.mkString(" "))
// val almostLemmaDF = cleanDF.withColumn("text", makeSTRING($"toks") ).drop("toks")
// val rawlemmasDF = almostLemmaDF.select('username, lemma('text).as('lemmas) )// .select('id, explode(ssplit('doc)).as('sen)).....tokenize('sen).as('words)
// rawlemmasDF.show(5)
//
// val cleanDF = rawtextDF.select('username, cleanxml('raw_text).as('doc)).select('username, explode(ssplit('doc)).as('sen)).select('username, tokenize('sen).as('toks))
//
// val rawLemmasDF = cleanDF.select('username,'lemmas).map( (x: String ,y: Array[String] ) => (x,y) ).reduceByKey(a,b => a++b).toDF("username","lemmas")

// val combSen = udf((x:Seq[String]) => x.filterNot(_.matches("\\W+")))
// val cleanDF = toksDF.withColumn("toks", combSen($"toks"))

val textDF = rawtextDF.select('username, cleanxml('raw_text).as('text))//.select('username, tokenize('text).as('toks))

//Tokenize text
val regexTokenizer = new RegexTokenizer().setInputCol("text").setOutputCol("toks").setPattern("\\W+")
val toksDF = regexTokenizer.transform(textDF)

//Remove Stopwords
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
