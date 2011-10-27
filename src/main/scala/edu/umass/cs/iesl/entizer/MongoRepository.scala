package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._

/**
 * @author kedar
 */

class MongoRepository(val dbName: String) {
  val dbHost: String = Conf.get[String]("mongo-host", "localhost")
  val dbPort: Int = Conf.get[Int]("mongo-port", 27017)

  // connect to db
  val mongoConn = MongoConnection(dbHost, dbPort)
  val mongoDB = mongoConn(dbName)

  // default collections
  lazy val mentionsColl = collection("mentions")

  def collection(name: String): MongoCollection = mongoDB(name)

  def clear() {
    mongoDB.dropDatabase()
  }
}

object MongoListHelper {
  def getListAttr[T](dbo: DBObject, key: String): Seq[T] =
    dbo.get(key).asInstanceOf[BasicDBList].map(_.asInstanceOf[T])

  def getListOfListAttr[T](dbo: DBObject, key: String): Seq[Seq[T]] =
    dbo.get(key).asInstanceOf[BasicDBList].map(_.asInstanceOf[BasicDBList].map(_.asInstanceOf[T]))
}
