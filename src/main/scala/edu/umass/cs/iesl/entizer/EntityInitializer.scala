package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._
import collection.mutable.{HashMap, HashSet}

/**
 * @author kedar
 */

trait MentionSegmentProcessor extends ParallelCollectionProcessor {
  def maxSegmentLength: Int

  def useOracle: Boolean

  def useFullSegment: Boolean

  def useAllRecordSegments: Boolean

  def fieldName: String

  def inputJob = JobCenter.Job(query = (if (useOracle) MongoDBObject() else MongoDBObject("isRecord" -> true)),
    select = MongoDBObject("isRecord" -> 1, "words" -> 1, "bioLabels" -> 1, "possibleEnds" -> 1, "cluster" -> 1))

  def getSegments(mention: Mention): Seq[TextSegment] = {
    if (useFullSegment) mention.fullSegments(fieldName)
    else {
      if (mention.isRecord) {
        if (useAllRecordSegments) mention.allTrueSegments()
        else mention.trueSegments(fieldName)
      }
      else mention.possibleSegments(fieldName, maxSegmentLength)
    }
  }
}

class EntityInitializer(val fieldName: String, val inputColl: MongoCollection, val fieldColl: MongoCollection,
                        val maxSegmentLength: Int, val isPhraseUnique: Boolean = false,
                        val useFullSegment: Boolean = false, val useAllRecordSegments: Boolean = false,
                        val useOracle: Boolean = false, val hashCodes: Seq[String] => Seq[String] = identity(_))
  extends MentionSegmentProcessor {
  // initialize indices for field collection
  // fieldColl.ensureIndex(MongoDBObject("mentionId" -> 1, "begin" -> 1, "end" -> 1), "mentionPhrase", unique = true)
  // fieldColl.ensureIndex(MongoDBObject("phrase" -> 1), "entityPhrase", unique = isPhraseUnique)
  // fieldColl.ensureIndex("mentionId")
  // fieldColl.ensureIndex("hashCodes")

  def name = "entityInitializer[name=" + fieldName + "]"

  def process(dbo: DBObject, inputParams: Any, partialOutputParams: Any) {
    val mention = new Mention(dbo)
    def addSegmentToCollection(segment: TextSegment) {
      // build entity value
      val builder = MongoDBObject.newBuilder
      val begin = segment.begin
      val end = segment.end
      val phrase = mention.words.slice(begin, end)
      val hashes = hashCodes(phrase).toSet.toSeq
      builder += "mentionId" -> mention.id
      builder += "begin" -> begin
      builder += "end" -> end
      builder += "isRecord" -> mention.isRecord
      if (mention.trueClusterOption.isDefined) builder += "cluster" -> mention.trueClusterOption.get
      builder += "phrase" -> phrase
      builder += "hashCodes" -> hashes
      fieldColl += builder.result()
    }
    // add segments based on true segmentation or all segmentations
    for (segment <- getSegments(mention)) addSegmentToCollection(segment)
  }
}

class EntityIdToPhraseProcessor(val fieldName: String, val phraseType: String,
                                val inputColl: MongoCollection) extends ParallelCollectionProcessor {
  def name = "entityIdToPhrase[name=" + fieldName + "][type=" + phraseType + "]"

  def inputJob = JobCenter.Job(select = MongoDBObject(phraseType -> 1))

  override def newOutputParams(isMaster: Boolean = false) = new HashMap[ObjectId, Seq[String]]

  override def merge(outputParams: Any, partialOutputParams: Any) {
    outputParams.asInstanceOf[HashMap[ObjectId, Seq[String]]] ++= partialOutputParams.asInstanceOf[HashMap[ObjectId, Seq[String]]]
  }

  def process(dbo: DBObject, inputParams: Any, partialOutputParams: Any) {
    val phrase = MongoHelper.getListAttr[String](dbo, phraseType)
    val partialOutput = partialOutputParams.asInstanceOf[HashMap[ObjectId, Seq[String]]]
    partialOutput(dbo._id.get) = phrase
  }
}

class EntityIdToMentionSegmentProcessor(val fieldName: String, val inputColl: MongoCollection)
extends ParallelCollectionProcessor {
  def name = "entityIdToMentionSegment[name=" + fieldName + "]"

  def inputJob = JobCenter.Job(select = MongoDBObject("mentionId" -> 1, "begin" -> 1, "end" -> 1))

  override def newOutputParams(isMaster: Boolean = false) = new HashMap[ObjectId, MentionSegment]

  override def merge(outputParams: Any, partialOutputParams: Any) {
    outputParams.asInstanceOf[HashMap[ObjectId, MentionSegment]] ++= partialOutputParams.asInstanceOf[HashMap[ObjectId, MentionSegment]]
  }

  def process(dbo: DBObject, inputParams: Any, partialOutputParams: Any) {
    val partialOutput = partialOutputParams.asInstanceOf[HashMap[ObjectId, MentionSegment]]
    val mentionId = dbo.get("mentionId").asInstanceOf[ObjectId]
    val begin = dbo.as[Int]("begin")
    val end = dbo.as[Int]("end")
    partialOutput(dbo._id.get) = MentionSegment(mentionId, begin, end)
  }
}

trait FieldMentionSegmentAlignProcessor extends MentionSegmentProcessor {
  def field: Field

  def maxSegmentLength = field.maxSegmentLength

  def useOracle = true

  def useFullSegment = field.useFullSegment

  def useAllRecordSegments = true

  def fieldName = field.name
}

class FieldMentionExtractPredicateProcessor(val inputColl: MongoCollection, val field: Field, val predicateName: String,
                                            val predicateFn: (Mention, Int, Int) => Boolean)
  extends FieldMentionSegmentAlignProcessor {
  def name = "extractionPredicate[field=" + fieldName + "][predicate=" + predicateName + "]"

  override def newOutputParams(isMaster: Boolean = false) = new ExtractSegmentPredicate(predicateName)

  override def merge(outputParams: Any, partialOutputParams: Any) {
    outputParams.asInstanceOf[ExtractSegmentPredicate] ++= partialOutputParams.asInstanceOf[ExtractSegmentPredicate]
  }

  def process(dbo: DBObject, inputParams: Any, partialOutputParams: Any) {
    val mention = new Mention(dbo)
    val partialExtractPredicate = partialOutputParams.asInstanceOf[ExtractSegmentPredicate]
    for (segment <- getSegments(mention) if predicateFn(mention, segment.begin, segment.end)) {
      partialExtractPredicate += MentionSegment(mention.id, segment.begin, segment.end)
    }
  }
}

class EntityFieldMentionPossibleValueProcessor(val inputColl: MongoCollection, val field: Field, val simThreshold: Double)
  extends FieldMentionSegmentAlignProcessor {
  val entityField = field.asInstanceOf[EntityField]
  val logN = math.log(1.0 * entityField.size)
  
  def name = "entityPossibleValues[field=" + fieldName + "]"

  // (mentionId, begin, end) -> Seq(valueId)
  override def newOutputParams(isMaster: Boolean = false) = new MentionSegmentToEntityValues

  override def merge(outputParams: Any, partialOutputParams: Any) {
    val outputAligns = outputParams.asInstanceOf[MentionSegmentToEntityValues]
    for ((mentionSegment, fieldValues) <- partialOutputParams.asInstanceOf[MentionSegmentToEntityValues]) {
      outputAligns(mentionSegment) = outputAligns.getOrElse(mentionSegment, new HashSet[FieldValue]) ++ fieldValues
    }
  }

  protected def toUnitVector(hashes: Seq[String]): HashMap[String, Double] = {
    val vec = new HashMap[String, Double]
    for (hash <- hashes) {
      vec(hash) = vec.getOrElse(hash, 0.0) + (logN - math.log(1.0 * entityField.hashToDocFreq.getOrElse(hash, 1)))
    }
    val norm = math.sqrt(vec.values.map(x => x * x).foldLeft(0.0)(_ + _))
    for ((hash, value) <- vec) vec(hash) = value / norm
    vec
  }

  protected def getSimilarity(h1: Seq[String], h2: Seq[String]): Double = {
    val (vec1, vec2) = {
      if (h1.size <= h2.size) (toUnitVector(h1), toUnitVector(h2))
      else (toUnitVector(h2), toUnitVector(h1))
    }
    var sim = 0.0
    for ((key, value) <- vec1 if vec2.contains(key)) sim += value * vec2(key)
    sim
  }

  def process(dbo: DBObject, inputParams: Any, partialOutputParams: Any) {
    val mention = new Mention(dbo)
    val partialAligns = partialOutputParams.asInstanceOf[MentionSegmentToEntityValues]
    // collect field values first
    for (segment <- getSegments(mention)) {
      val phrase = mention.words.slice(segment.begin, segment.end)
      val phraseHashes = entityField.hashCodes(phrase)
      val allFieldValues = new HashSet[FieldValue]
      for (phraseHash <- phraseHashes if entityField.hashToIds.contains(phraseHash)) {
        allFieldValues ++= entityField.hashToIds(phraseHash).map(id => FieldValue(entityField, Some(id)))
      }
      val mentionSegment = MentionSegment(mention.id, segment.begin, segment.end)
      for (fieldValue <- allFieldValues) {
        val valueHashes = entityField.getValueHashes(fieldValue.valueId)
        if (getSimilarity(valueHashes, phraseHashes) >= simThreshold) {
          partialAligns(mentionSegment) = partialAligns.getOrElse(mentionSegment, new HashSet[FieldValue]) ++ Seq(fieldValue)
        }
      }
    }
  }
}

class FieldMentionAlignPredicateProcessor(val inputColl: MongoCollection, val field: Field, val predicateName: String,
                                          val predicateFn: (FieldValue, Mention, Int, Int) => Boolean)
  extends FieldMentionSegmentAlignProcessor {
  def name = "alignmentPredicate[field=" + fieldName + "][predicate=" + predicateName + "]"

  override def newOutputParams(isMaster: Boolean = false) = new AlignSegmentPredicate(predicateName)

  override def merge(outputParams: Any, partialOutputParams: Any) {
    outputParams.asInstanceOf[AlignSegmentPredicate] ++= partialOutputParams.asInstanceOf[AlignSegmentPredicate]
  }

  def process(dbo: DBObject, inputParams: Any, partialOutputParams: Any) {
    val mention = new Mention(dbo)
    val partialAlignPredicate = partialOutputParams.asInstanceOf[AlignSegmentPredicate]
    for (segment <- getSegments(mention)) {
      val mentionSegment = MentionSegment(mention.id, segment.begin, segment.end)
      for (fieldValue <- field.getPossibleValues(mention.id, segment.begin, segment.end)
           if predicateFn(fieldValue, mention, segment.begin, segment.end)) {
        partialAlignPredicate += FieldValueMentionSegment(fieldValue, mentionSegment)
      }
    }
  }
}
