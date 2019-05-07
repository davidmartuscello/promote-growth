
val siteList = Seq("fitness.stackexchange.com.7z", "health.stackexchange.com.7z","biology.stackexchange.com.7z","graphicdesign.stackexchange.com.7z")

val firstRDD = getTextDF(siteList(0))
val secondRDD = getTextDF(siteList(1))
//val thirdRDD = getTextDF(siteList(2))
//val fourthRDD = getTextDF(siteList(3))
val combinedRDD = firstRDD.union(secondRDD)//.union(thirdRDD).union(fourthRDD)
val reducedRDD = combinedRDD.reduceByKey((a: String,b: String) => a + b)
reducedRDD.take(1)


//Get Target String
val proposalLines = sc.textFile("hdfs:///user/dm4350/project/Proposal_Example_Questions.txt")

val proposalString = proposalLines.reduce( (x,y) => x+y )

val proposalKey = sc.parallelize( Seq( ("PROPOSAL", proposalString) ) )

val finalRDD = textRDD.union(proposalKey)

// val siteList = Seq("fitness.stackexchange.com.7z", "health.stackexchange.com.7z","biology.stackexchange.com.7z","graphicdesign.stackexchange.com.7z")
// val siteListRDD = sc.parallelize(siteList)
// val combinedRDD = siteListRDD.map(getTextDF(_)).reduce( (a,b) => a.union(b) )
// val reducedRDD = fullStringRDD.reduceByKey((a: String,b: String) => a + b)
//
// var fullRDD = sc.parallelize(Seq())
// for name in siteList:
//   val newRDD = getTextDF(name)
//   fullRDD = fullRDD.union(newRDD)
//
// var fullRDD = sc.parallelize( Seq("test" -> "nothing") )
// siteList.foreach( name => fullRDD.union(getTextDF(name)) )
// fullRDD.take(1)
