package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._
import collection.mutable.{ArrayBuffer, HashMap, HashSet}
import java.util.Random

/**
 * @author kedar
 */

case class MentionSegment(mentionId: ObjectId, begin: Int, end: Int)

class MentionSegmentToEntityValues extends HashMap[MentionSegment, HashSet[FieldValue]]

case class FieldValueMentionSegment(fieldValue: FieldValue, mentionSegment: MentionSegment)

case class SimpleField(name: String) extends Field {
  val isKey = false
  val useFullSegment = false
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

trait EntityField extends Field {
  var hashToDocFreq: HashMap[String, Int] = null
  var hashToIds: HashMap[String, Seq[ObjectId]] = null
  var numPhraseDuplicates: Int = 50
  var idToPhrase: HashMap[ObjectId, Seq[String]] = null
  var idToHashCodes: HashMap[ObjectId, Seq[String]] = null
  var idToMentionSegment: HashMap[ObjectId, MentionSegment] = null
  var mentionIdToValueIds: HashMap[ObjectId, Seq[ObjectId]] = null
  var hashCodes: Seq[String] => Seq[String] = PhraseHash.ngramWordHash(_, 1).toSeq
  var segmentToValues: MentionSegmentToEntityValues = null
  // canopy parameters
  var minSimilarity = 0.1
  var maxSimilarity = 0.9
  var maxHashFraction = 0.1

  def repository: MongoRepository

  def mentionColl: MongoCollection = repository.mentionColl

  def entityColl: MongoCollection = repository.collection(name + "_entity")

  def setMaxSegmentLength(maxSegLen: Int = Short.MaxValue.toInt) = {
    maxSegmentLength = maxSegLen
    this
  }

  def setPhraseUnique(isPhraseUniq: Boolean = false) = {
    if (isPhraseUniq) numPhraseDuplicates = 1
    this
  }

  def setPhraseDuplicates(numPhraseDups: Int = 50) = {
    numPhraseDuplicates = numPhraseDups
    this
  }

  def setHashCodes(hashCoder: Seq[String] => Seq[String] = PhraseHash.ngramWordHash(_, 1).toSeq) = {
    hashCodes = hashCoder
    this
  }

  def setSimilarities(minSim: Double = 0.1, maxSim: Double = 0.9) = {
    minSimilarity = minSim
    maxSimilarity = maxSim
    this
  }

  def setMaxHashFraction(maxHashFrac: Double = 0.1) = {
    maxHashFraction = maxHashFrac
    this
  }

  def rehash() = {
    // set up document frequency
    hashToDocFreq = new HashDocFreqProcessor(name, entityColl).run().asInstanceOf[HashMap[String, Int]]
    // set up inverted index
    val tmpHashToIds = new HashInvIndexProcessor(name, entityColl).run().asInstanceOf[HashMap[String, Seq[ObjectId]]]
    // remove hashes that link to more than maxfraction of entities
    val maxIds = (maxHashFraction * size).toInt
    logger.info("pruning entities[%s] with #ids > %d (fraction=%.3f, size=%d)".format(name, maxIds, maxHashFraction, size))
    val emptyIds = Seq.empty[ObjectId]
    hashToIds = new HashMap[String, Seq[ObjectId]]
    for ((hash, ids) <- tmpHashToIds) {
      hashToIds(hash) = {
        if (ids.size > maxIds) {
          logger.info("pruning hash='%s'".format(hash))
          emptyIds
        } else {
          ids
        }
      }
    }
    // get id -> phrase mapping
    idToPhrase = new EntityIdToPhraseProcessor(name, "phrase", entityColl).run().asInstanceOf[HashMap[ObjectId, Seq[String]]]
    // get id -> hash code mapping
    idToHashCodes = new EntityIdToPhraseProcessor(name, "hashCodes", entityColl).run().asInstanceOf[HashMap[ObjectId, Seq[String]]]
    // get id -> mention segment mapping
    idToMentionSegment = new EntityIdToMentionSegmentProcessor(name, entityColl).run().asInstanceOf[HashMap[ObjectId, MentionSegment]]
    // get mentionId -> Seq(ids)
    mentionIdToValueIds = new HashMap[ObjectId, Seq[ObjectId]]
    for ((id, segment) <- idToMentionSegment) {
      mentionIdToValueIds(segment.mentionId) = mentionIdToValueIds.getOrElse(segment.mentionId, Seq.empty[ObjectId]) ++ Seq(id)
    }
    // set up possible values index
    val segmentToAllValues = new EntityFieldMentionPossibleValueProcessor(mentionColl, this, minSimilarity).run().asInstanceOf[MentionSegmentToEntityValues]
    val segmentToCoreValues = new EntityFieldMentionPossibleValueProcessor(mentionColl, this, maxSimilarity).run().asInstanceOf[MentionSegmentToEntityValues]
    val valueToPossibleCoreValues = new HashMap[FieldValue, HashSet[FieldValue]]
    val rnd = new java.util.Random()
    var allValues = new ArrayBuffer[FieldValue]
    allValues ++= idToPhrase.keys.map(id => FieldValue(this, Some(id)))
    // def simpleValueString(v: FieldValue) = getValuePhrase(v.valueId).mkString("'", " ", "'")
    while (allValues.size > 0) {
      val canopyValue = allValues(rnd.nextInt(allValues.size))
      val canopySegment = idToMentionSegment(canopyValue.valueId.get)
      // canopy core
      val canopyCoreValues = segmentToCoreValues(canopySegment)
      val canopyAllValues = segmentToAllValues(canopySegment)
      val prunedCoreValues = canopyCoreValues.toSeq.sortWith(_.valueId.hashCode() < _.valueId.hashCode())
        .take(numPhraseDuplicates - 1) ++ Seq(canopyValue)
      // logger.info("Canopy value: " + simpleValueString(canopyValue) + " core=" + canopyCoreValues.map(simpleValueString(_)).mkString("[", ", ", "]"))
      if (prunedCoreValues.size < canopyCoreValues.size) {
        logger.info("Filtering '%s' before=%d, after=%d".format(canopyValue.toString,
          canopyCoreValues.size, prunedCoreValues.size))
      }
      require(prunedCoreValues.size > 0, "#coreValues=0 for canopy value=" + canopyValue)
      for (otherCanopyValue <- canopyAllValues) {
        // logger.info("Generating " + simpleValueString(otherCanopyValue) + " using " + prunedCoreValues.map(simpleValueString(_)).mkString("[", ", ", "]"))
        valueToPossibleCoreValues(otherCanopyValue) = valueToPossibleCoreValues.getOrElse(otherCanopyValue,
          new HashSet[FieldValue]) ++ prunedCoreValues
      }
      allValues = allValues.diff(canopyCoreValues.toSeq)
    }
    // for each value select a small set of core values
    segmentToValues = new MentionSegmentToEntityValues
    for ((segment, values) <- segmentToAllValues) {
      val segmentRelatedValues = new HashSet[FieldValue]
      for (value <- values) segmentRelatedValues ++= valueToPossibleCoreValues(value)
      segmentToValues(segment) = segmentRelatedValues
    }
    this
  }

  def init() = {
    entityColl.drop()
    segmentToValues = null
    // first add record fields to field collection
    new EntityInitializer(name, mentionColl, entityColl, maxSegmentLength, false, useFullSegment,
      useAllRecordSegments, useOracle, hashCodes).run()
    logger.info("#entities[%s]=%d".format(name, size))
    rehash()
  }

  def getHashDocumentFrequency = hashToDocFreq

  def getPossibleValues(mentionId: ObjectId, begin: Int, end: Int) = {
    (segmentToValues.getOrElse(MentionSegment(mentionId, begin, end), new HashSet[FieldValue]) ++
      Seq(FieldValue(this, None))).toSeq
  }

  def getValuePhrase(valueId: Option[ObjectId]) = {
    if (valueId.isDefined) {
      if (idToPhrase == null) {
        val dbo = entityColl.findOneByID(valueId.get, MongoDBObject("phrase" -> 1)).get
        MongoHelper.getListAttr[String](dbo, "phrase")
      } else idToPhrase(valueId.get)
    } else Seq.empty[String]
  }

  def getValueHashes(valueId: Option[ObjectId]) = {
    if (valueId.isDefined) {
      if (idToHashCodes == null) {
        val dbo = entityColl.findOneByID(valueId.get, MongoDBObject("hashCodes" -> 1)).get
        MongoHelper.getListAttr[String](dbo, "hashCodes")
      } else idToHashCodes(valueId.get)
    } else Seq.empty[String]
  }

  def getValueMention(valueId: Option[ObjectId]) = {
    if (valueId.isDefined) Some(idToMentionSegment(valueId.get).mentionId) else None
  }

  def getValueMentionSegment(valueId: Option[ObjectId]) = {
    if (valueId.isDefined) Some(idToMentionSegment(valueId.get)) else None
  }

  def getMentionValues(mentionId: Option[ObjectId]) = {
    val mentionValBuff = new HashSet[FieldValue]
    if (mentionId.isDefined) {
      mentionValBuff ++= mentionIdToValueIds.getOrElse(mentionId.get, Seq.empty[ObjectId])
        .map(id => FieldValue(this, Some(id)))
    }
    mentionValBuff.toSeq
  }

  def size: Int = entityColl.count.toInt
}

class SimpleEntityField(val name: String, val repository: MongoRepository,
                        val isKey: Boolean = true) extends EntityField {
  val useFullSegment = false
  val useAllRecordSegments = false
  val useOracle = false
}