import scala.xml._

def getNodes(xmlstring: String): Node = {
  XML.loadString(xmlstring)
}

def getUsers(users: Node): NodeSeq = {
 (users \ "row")
}

def xmlToRDD(inputRDD : org.apache.spark.rdd.RDD[(String, String)] , filename : String, foldername: String) = {
  val string = inputRDD.filter(tup => tup._1 == "hdfs://dumbo/user/dm4350/project/allsites/" + foldername + "FOLDER/"+filename).map(tup => tup._2).take(1)(0)
  val array = getNodes(string.slice(1,string.length)) //get rid of rogue "?" at beginning of string
  val rdd = sc.parallelize(array.descendant)
  rdd
}

def getTextDF(folderName: String) = {
  val siteRDD = sc.wholeTextFiles("hdfs:///user/dm4350/project/allsites/" + folderName + "FOLDER")

  val empty = <p></p>
  val zero = <p>0</p>
  val usersRDD = xmlToRDD(siteRDD, "Users.xml", folderName)
  val usersSubRDD = usersRDD.filter(row => row.attribute("Id")!= None).map(
    user => (
      user.attribute("Id").getOrElse(zero).text.toInt,
      user.attribute("AboutMe").getOrElse(empty).text) )//, user.attribute("Reputation")

  val postsRDD = xmlToRDD(siteRDD, "Posts.xml", folderName)
  val postsSubRDD = postsRDD.filter(
    row => row.attribute("Id")!= None).map(
      post => (
        post.attribute("OwnerUserId").getOrElse(zero).text.toInt, // post.attribute("PostTypeId"),
        post.attribute("Tags").getOrElse(empty).text.replaceAll("[&lt][&gt]<>","").replaceAll(";"," ") +
        post.attribute("Body").getOrElse(empty).text) )
  val tagsRDD = postsSubRDD.filter(tup => tup._2 != None).map(tup => tup._2  )

  val commentsRDD = xmlToRDD(siteRDD, "Comments.xml", folderName)
  val commentsSubRDD = commentsRDD.filter(
    row => row.attribute("Id")!= None).map(
      comment => (
      comment.attribute("UserId").getOrElse(zero).text.toInt,
      comment.attribute("Text").getOrElse(empty).text) )

  val usernameMap = usersRDD.map(row => row.attribute("Id").getOrElse(zero).text.toInt->row.attribute("DisplayName").getOrElse(empty).text ).collectAsMap

  val fullStringRDD = usersSubRDD.union(postsSubRDD).union(commentsSubRDD)
  val reducedRDD = fullStringRDD.reduceByKey((a: String,b: String) => a + b)
  val filteredRDD = reducedRDD.filter(tup => tup._2 != "")

  val usernameRDD = filteredRDD.map( tup => usernameMap.get(tup._1).getOrElse("null")->tup._2 )
  usernameRDD
}
