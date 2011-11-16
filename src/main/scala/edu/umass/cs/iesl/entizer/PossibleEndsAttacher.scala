package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._

/**
 * @author kedar
 */

abstract class PossibleEndsAttacher(val inputColl: MongoCollection, val name: String) extends ParallelCollectionProcessor {
  def inputJob = JobCenter.Job(select = MongoDBObject("words" -> 1))

  def getPossibleEnds(words: Array[String]): Array[Boolean]

  def process(dbo: DBObject, inputParams: Any, partialOutputParams: Any) {
    val _id: ObjectId = dbo._id.get
    val words = MongoHelper.getListAttr[String](dbo, "words").toArray
    val ends = getPossibleEnds(words)
    inputColl.update(MongoDBObject("_id" -> _id), $set("possibleEnds" -> ends), false, false)
  }
}

abstract class FeaturesAttacher(val inputColl: MongoCollection, val name: String) extends ParallelCollectionProcessor {
  def inputJob = JobCenter.Job(select = MongoDBObject("words" -> 1))

  def getFeatures(words: Array[String]): Seq[Seq[String]]

  def process(dbo: DBObject, inputParams: Any, partialOutputParams: Any) {
    val _id: ObjectId = dbo._id.get
    val words = MongoHelper.getListAttr[String](dbo, "words").toArray
    val features = getFeatures(words)
    inputColl.update(MongoDBObject("_id" -> _id), $set("features" -> features), false, false)
  }
}

class SchemaNormalizer(val inputColl: MongoCollection, val schemaMappings: Map[String, String])
  extends ParallelCollectionProcessor {
  def name = "schemaNormalizer[transforms=" + schemaMappings + "]"

  // process all mentions
  def inputJob = JobCenter.Job(select = MongoDBObject("bioLabels" -> 1))

  // override def debugEvery = 1000

  def process(dbo: DBObject, inputParams: Any, partialOutputParams: Any) {
    val _id: ObjectId = dbo._id.get
    val labels = MongoHelper.getListAttr[String](dbo, "bioLabels").toArray
    for (i <- 0 until labels.length) {
      if (schemaMappings.contains(labels(i))) {
        labels(i) = schemaMappings(labels(i))
      }
    }
    inputColl.update(MongoDBObject("_id" -> _id), $set("bioLabels" -> labels), false, false)
  }
}