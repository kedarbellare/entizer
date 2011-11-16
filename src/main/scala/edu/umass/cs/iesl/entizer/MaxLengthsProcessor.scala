package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._
import collection.mutable.{HashSet, HashMap}

/**
 * @author kedar
 */

class MaxLengthsProcessor(val inputColl: MongoCollection,
                          val useOracle: Boolean = false) extends ParallelCollectionProcessor {
  def name = "maxLengthFinder[oracle=" + useOracle + "]"

  def inputJob = {
    if (useOracle)
      JobCenter.Job(select = MongoDBObject("isRecord" -> 1, "bioLabels" -> 1))
    else
      JobCenter.Job(query = MongoDBObject("isRecord" -> true), select = MongoDBObject("isRecord" -> 1, "bioLabels" -> 1))
  }

  override def newOutputParams(isMaster: Boolean = false) = {
    val lblToMaxLength = new HashMap[String, Int]
    lblToMaxLength("O") = Short.MaxValue.toInt
    lblToMaxLength
  }

  override def merge(outputParams: Any, partialOutputParams: Any) {
    val outputMaxLengths = outputParams.asInstanceOf[HashMap[String, Int]]
    for ((lbl, maxlen) <- partialOutputParams.asInstanceOf[HashMap[String, Int]]) {
      outputMaxLengths(lbl) = math.max(maxlen, outputMaxLengths.getOrElse(lbl, 1))
    }
  }

  def process(dbo: DBObject, inputParams: Any, partialOutputParams: Any) {
    if (useOracle || dbo.as[Boolean]("isRecord")) {
      val partialMaxLengths = partialOutputParams.asInstanceOf[HashMap[String, Int]]
      val labels = MongoHelper.getListAttr[String](dbo, "bioLabels").toArray
      val segments = TextSegmentationHelper.getTextSegmentationFromBIO(labels)
      for (segment <- segments) {
        partialMaxLengths(segment.label) = math.max(segment.end - segment.begin, partialMaxLengths.getOrElse(segment.label, 1))
      }
    }
  }
}


class UniqueClusterProcessor(val inputColl: MongoCollection) extends ParallelCollectionProcessor {
  def name = "uniqueClusters"

  def inputJob = JobCenter.Job(select = MongoDBObject("cluster" -> 1))

  override def newOutputParams(isMaster: Boolean = false) = new HashSet[String]

  override def merge(outputParams: Any, partialOutputParams: Any) {
    outputParams.asInstanceOf[HashSet[String]] ++= partialOutputParams.asInstanceOf[HashSet[String]]
  }

  def process(dbo: DBObject, inputParams: Any, partialOutputParams: Any) {
    for (cluster <- dbo.getAs[String]("cluster")) {
      partialOutputParams.asInstanceOf[HashSet[String]] += cluster
    }
  }
}