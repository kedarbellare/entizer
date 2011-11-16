package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._

/**
 * @author kedar
 */

object MongoHelper {
  def getAttr[T](dbo: DBObject, key: String): T = dbo.get(key).asInstanceOf[T]

  def getListAttr[T](dbo: DBObject, key: String): Seq[T] =
    dbo.get(key).asInstanceOf[BasicDBList].map(_.asInstanceOf[T])

  def getListOfListAttr[T](dbo: DBObject, key: String): Seq[Seq[T]] =
    dbo.get(key).asInstanceOf[BasicDBList].map(_.asInstanceOf[BasicDBList].map(_.asInstanceOf[T]))
}

class MongoRepository(val dbName: String) {
  private val dbHost: String = Conf.get[String]("mongo-host", "localhost")
  private val dbPort: Int = Conf.get[Int]("mongo-port", 27017)

  // connect to db
  private val mongoConn = MongoConnection(dbHost, dbPort)
  private val mongoDB = mongoConn(dbName)

  // default collections
  lazy val mentionColl = collection("mentions")

  def collection(name: String): MongoCollection = mongoDB(name)

  def clear() {
    mongoDB.dropDatabase()
  }
}