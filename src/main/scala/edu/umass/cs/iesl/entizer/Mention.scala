package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._
import cc.refectorie.user.kedarb.dynprog.AExample
import MongoHelper._
import TextSegmentationHelper._
import collection.mutable.{HashSet, ArrayBuffer}

/**
 * @author kedar
 */

case class Mention(id: ObjectId, isRecord: Boolean, words: Seq[String], possibleEnds: Seq[Boolean],
                   trueWidget: TextSegmentation, trueClusterOption: Option[String]) extends AExample[TextSegmentation] {
  var features: Seq[Seq[String]] = null
  var alignFeatures: HashSet[AlignmentFeature] = null

  def this(dbo: DBObject) = {
    this (dbo._id.get, getAttr[Boolean](dbo, "isRecord"), getListAttr[String](dbo, "words"),
      getListAttr[Boolean](dbo, "possibleEnds"), getTextSegmentationFromBIO(getListAttr[String](dbo, "bioLabels")),
      dbo.getAs[String]("cluster"))
  }

  def setFeatures(dbo: DBObject) = {
    if (dbo.contains("features")) features = getListOfListAttr[String](dbo, "features")
    this
  }

  def setAlignFeatures(dbo: DBObject) = {
    if (dbo.contains("alignFeatures")) {
      alignFeatures = new HashSet[AlignmentFeature]()
      for (featDbo <- MongoHelper.getListAttr[DBObject](dbo, "alignFeatures")) {
        val predicateName = featDbo.as[String]("name")
        val fieldType = featDbo.as[String]("field")
        val valueName = featDbo.as[String]("value")
        val begin = featDbo.as[Int]("begin")
        val end = featDbo.as[Int]("end")
        alignFeatures += AlignmentFeature(predicateName, fieldType, valueName, this.id, begin, end)
      }
    }
    this
  }

  def trueSegments(fieldName: String): Seq[TextSegment] = trueWidget.filter(_.label == fieldName)

  def allTrueSegments(): Seq[TextSegment] = trueWidget.toSeq

  def possibleSegments(fieldName: String, maxSegmentLength: Int): Seq[TextSegment] = {
    val N = words.length
    val segmentBuff = new ArrayBuffer[TextSegment]
    for (begin <- 0 until N if begin == 0 || possibleEnds(begin)) {
      for (end <- (begin + 1) until math.min(begin + maxSegmentLength, N) + 1 if possibleEnds(end)) {
        segmentBuff += TextSegment(fieldName, begin, end)
      }
    }
    segmentBuff.toSeq
  }

  def fullSegments(fieldName: String): Seq[TextSegment] = Seq(TextSegment(fieldName, 0, words.length))

  def numTokens = words.length
}