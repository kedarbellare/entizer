package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._
import scala.util.Random
import collection.mutable.{HashSet, ArrayBuffer, HashMap}

/**
 * @author kedar
 */

trait FieldCollection extends Field {
  val fieldNameIndexer = new HashMap[String, Int]
  val fields = new ArrayBuffer[Field]

  def addField(field: Field) = {
    if (!fieldNameIndexer.contains(field.name)) {
      fieldNameIndexer(field.name) = fields.size
      fields += field
    }
    this
  }

  def indexOf(name: String) = fieldNameIndexer(name)

  def numFields = fields.size

  def getField(index: Int) = fields(index)

  def getField(name: String) = fields(indexOf(name))

  def getFields: Seq[Field] = fields
}

case class SimpleRecord(name: String) extends FieldCollection {
  val isKey = false
  val useFullSegment = true
  val useAllRecordSegments = false
  val useOracle = false

  def setMaxSegmentLength(maxSegLen: Int = Short.MaxValue.toInt) = {
    maxSegmentLength = maxSegLen
    this
  }

  def init() = this

  def getPossibleValues(mentionId: ObjectId, begin: Int, end: Int) = Seq(FieldValue(this, None))

  def getMentionValues(mentionId: Option[ObjectId]) = Seq(FieldValue(this, None))

  def getValuePhrase(valueId: Option[ObjectId]) = Seq.empty[String]

  def getValueMention(valueId: Option[ObjectId]) = None

  def getValueMentionSegment(valueId: Option[ObjectId]) = None
}

class SimpleEntityRecord(val name: String, val repository: MongoRepository,
                         val useOracle: Boolean = false) extends EntityField with FieldCollection {
  val isKey = false
  val useFullSegment = true
  val useAllRecordSegments = false
}

class ClusterEntityRecord(val name: String, val repository: MongoRepository) extends EntityField {
  val isKey = false
  val useFullSegment = true
  val useAllRecordSegments = false
  val useOracle = true

  def getRecordClusters = {
    val rnd = new Random
    val allValues = entityColl.find(MongoDBObject(), MongoDBObject()).map(dbo => FieldValue(this, Some(dbo._id.get))).toSeq
    ccPivot(rnd, allValues)
  }

  private def ccPivot(rnd: Random, values: Seq[FieldValue]): Seq[HashSet[FieldValue]] = {
    if (values.length == 0) Seq.empty[HashSet[FieldValue]]
    else {
      // pick random value id
      val pivotIndex = rnd.nextInt(values.length)
      val pivotValueId = values(pivotIndex).valueId
      // get all segments matching pivot mention segment
      val pivotMentionSegment = getValueMentionSegment(pivotValueId)
      val currCluster = new HashSet[FieldValue]
      currCluster += values(pivotIndex)
      for (segment <- pivotMentionSegment) {
        currCluster ++= getPossibleValues(segment.mentionId, segment.begin, segment.end).filter(_.valueId.isDefined)
      }

      val otherValues = values.filter(!currCluster(_))
      ccPivot(rnd, otherValues) ++ Seq(currCluster)
    }
  }
}
