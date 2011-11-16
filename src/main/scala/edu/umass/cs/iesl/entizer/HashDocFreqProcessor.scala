package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._
import collection.mutable.HashMap

/**
 * @author kedar
 */

class HashDocFreqProcessor(val fieldName: String, val inputColl: MongoCollection) extends ParallelCollectionProcessor {
  def name = "hashDocumentFrequency[field=" + fieldName + "]"

  def inputJob = JobCenter.Job(select = MongoDBObject("hashCodes" -> 1))

  override def newOutputParams(isMaster: Boolean = false) = new HashMap[String, Int]

  override def merge(outputParams: Any, partialOutputParams: Any) {
    val outputDocFreq = outputParams.asInstanceOf[HashMap[String, Int]]
    for ((code, count) <- partialOutputParams.asInstanceOf[HashMap[String, Int]]) {
      outputDocFreq(code) = count + outputDocFreq.getOrElse(code, 0)
    }
  }

  def process(dbo: DBObject, inputParams: Any, partialOutputParams: Any) {
    val partialOutputDocFreq = partialOutputParams.asInstanceOf[HashMap[String, Int]]
    val codeSet = Set.empty[String] ++ MongoHelper.getListAttr[String](dbo, "hashCodes")
    for (code <- codeSet.iterator) {
      partialOutputDocFreq(code) = partialOutputDocFreq.getOrElse(code, 0) + 1
    }
  }
}

class HashInvIndexProcessor(val fieldName: String, val inputColl: MongoCollection) extends ParallelCollectionProcessor {
  def name = "hashDocumentFrequency[field=" + fieldName + "]"

  def inputJob = JobCenter.Job(select = MongoDBObject("hashCodes" -> 1))

  override def newOutputParams(isMaster: Boolean = false) = new HashMap[String, Seq[ObjectId]]

  override def merge(outputParams: Any, partialOutputParams: Any) {
    val outputInvIdx = outputParams.asInstanceOf[HashMap[String, Seq[ObjectId]]]
    for ((code, ids) <- partialOutputParams.asInstanceOf[HashMap[String, Seq[ObjectId]]]) {
      outputInvIdx(code) = outputInvIdx.getOrElse(code, Seq.empty[ObjectId]) ++ ids
    }
  }

  def process(dbo: DBObject, inputParams: Any, partialOutputParams: Any) {
    val partialOutputInvIdx = partialOutputParams.asInstanceOf[HashMap[String, Seq[ObjectId]]]
    val codeSet = Set.empty[String] ++ MongoHelper.getListAttr[String](dbo, "hashCodes")
    for (code <- codeSet.iterator) {
      partialOutputInvIdx(code) = partialOutputInvIdx.getOrElse(code, Seq.empty[ObjectId]) ++ Seq(dbo._id.get)
    }
  }
}