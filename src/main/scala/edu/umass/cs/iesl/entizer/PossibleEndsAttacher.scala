package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._

/**
 * @author kedar
 */

abstract class PossibleEndsAttacher(val inputColl: MongoCollection, val name: String) extends ParallelCollectionProcessor {
  // only run for texts
  def inputJob = JobCenter.Job(query = MongoDBObject("isRecord" -> false), select = MongoDBObject("words" -> 1))

  def getPossibleEnds(words: Array[String]): Array[Boolean]

  def process(dbo: DBObject, inputParams: Any, partialOutputParams: Any) {
    val _id: ObjectId = dbo._id.get
    val words = MongoListHelper.getListAttr[String](dbo, "words").toArray
    val ends = getPossibleEnds(words)
    inputColl.update(MongoDBObject("_id" -> _id), $set("possibleEnds" -> ends), false, false)
  }
}