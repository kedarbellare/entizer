package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._

/**
 * @author kedar
 */


class SchemaNormalizer(val inputColl: MongoCollection, val schemaMappings: Map[String, String])
  extends ParallelCollectionProcessor {
  def name = "schemaNormalizer[transforms=" + schemaMappings + "]"

  // process all mentions
  def inputJob = JobCenter.Job(select = MongoDBObject("bioLabels" -> 1))

  // override def debugEvery = 1000

  def process(dbo: DBObject, inputParams: Any, partialOutputParams: Any) {
    val _id: ObjectId = dbo._id.get
    val labels = MongoListHelper.getListAttr[String](dbo, "bioLabels").toArray
    for (i <- 0 until labels.length) {
      if (schemaMappings.contains(labels(i))) {
        labels(i) = schemaMappings(labels(i))
      }
    }
    inputColl.update(MongoDBObject("_id" -> _id), $set("bioLabels" -> labels), false, false)
  }
}