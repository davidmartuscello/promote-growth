def getUsersRDD(folderName: String) = {
  val siteRDD = sc.wholeTextFiles("hdfs:///user/dm4350/project/allsites/" + folderName + "FOLDER")

  val empty = <p></p>
  val zero = <p>0</p>
  val usersRDD = xmlToRDD(siteRDD, "Users.xml", folderName)
  val usersSubRDD = usersRDD.filter(row => row.attribute("Id")!= None).map(
    user => (
      user.attribute("Id").getOrElse(zero).text.toInt,
      user.attribute("DisplayName").getOrElse(empty).text) )

  usersSubRDD
}

val site1 = siteList(0).split(".")(0)
val site2 = siteList(1).split(".")(0)
val usersRDD = getUsersRDD(siteList(0))
usersRDD.cache()

def getSite(username: String): String = {
  if usersRDD.filter(a => a._2 == username).count() > 0:
    return site1
  else:
    return site2
}

sortedUsers.withColumn("Site", getSite($Username))

sortedRDD.map(user => getSite)
