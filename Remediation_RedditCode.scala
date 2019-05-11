import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val cols = Seq("_c0", "_c1")

val doubleCols = Set("_c1")

val schema =  StructType(cols.map(c => StructField(c, if (doubleCols contains c) DoubleType else StringType)))

import org.apache.spark.sql.SparkSession

val spark: SparkSession = SparkSession.builder.master("local").getOrCreate

//sortedUsers.coalesce(1).write.csv("hdfs:///user/ttc290/project/user_list")

val user_df = spark.read.option("header", "false").schema(schema).csv("project/user_list/part-00000-84010f2b-25f0-4317-bb5e-018568212f22-c000.csv")

val newNames = Seq("User", "Score")

val dfRenamed = user_df.toDF(newNames: _*)

val user_list = dfRenamed.filter(!isnan($"Score"))

val user = user_list.filter($"User" =!= "[deleted]")

val potential_user = user.filter($"Score" > 0.7).select("User")

val userlist = potential_user.select("User").rdd.map(r => r(0).asInstanceOf[String]).collect.toList

val profileList = userlist.map( x => "https://www.reddit.com/user/" + x)

sc.parallelize(profileList).saveAsTextFile("hdfs:///user/ttc290/project/profile_list")

val msgList = userlist.map( x => "https://www.reddit.com/message/compose/?to=" + x)

sc.parallelize(msgList).saveAsTextFile("hdfs:///user/ttc290/project/msg_list")
