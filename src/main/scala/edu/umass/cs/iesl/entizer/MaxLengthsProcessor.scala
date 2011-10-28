package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._
import collection.mutable.HashMap

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
      if (lbl != "O") {
        outputMaxLengths(lbl) = math.max(maxlen, outputMaxLengths.getOrElse(lbl, 1))
      }
    }
  }

  def process(dbo: DBObject, inputParams: Any, partialOutputParams: Any) {
    if (useOracle || dbo.as[Boolean]("isRecord")) {
      val partialMaxLengths = partialOutputParams.asInstanceOf[HashMap[String, Int]]
      val labels = MongoListHelper.getListAttr[String](dbo, "bioLabels").toArray
      var currLbl = "O"
      var currMaxLen = 0
      for (lbl <- labels) {
        if (lbl == "O" || lbl.startsWith("B-")) {
          if (currLbl != "O") {
            partialMaxLengths(currLbl) = math.max(currMaxLen, partialMaxLengths.getOrElse(currLbl, 1))
          }
          currLbl = if (lbl == "O") lbl else lbl.substring(2)
          currMaxLen = 1
        } else if (lbl.startsWith("I-") && currLbl == lbl.substring(2)) {
          currMaxLen += 1
        } else {
          throw new RuntimeException("ERROR: curr=" + lbl + " and prev=" + currLbl + " combination unexpected!!")
        }
      }
    }
  }
}