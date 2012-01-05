package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._
import collection.mutable.{ArrayBuffer, HashMap, HashSet}
import net.spy.memcached.MemcachedClient
import java.net.InetSocketAddress
import org.riedelcastro.nurupo.HasLogger
import System.{currentTimeMillis => now}
import scala.util.Random
import java.io.IOException
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.util.Version
import org.apache.lucene.document.{Document, Field => LuceneField}
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.queryParser.QueryParser
import org.apache.lucene.store.RAMDirectory
import org.apache.lucene.index.{IndexWriterConfig, IndexReader, IndexWriter}

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

object EntizerMemcachedClient extends MemcachedClient(
  new InetSocketAddress(Conf.get[String]("memcached-host", "localhost"), Conf.get[Int]("memcached-port", 11211))) with HasLogger {
  def entityGet[T <: AnyRef](fieldName: String, id: ObjectId, keyName: String,
                             default: String => T, expire: Int = 3600): T = {
    val key = fieldName + "_id=" + id + "_key=" + keyName
    var ret = null.asInstanceOf[T]
    try {
      ret = get(key).asInstanceOf[T]
    } catch {
      case toe: Exception => {
        logger.info("Caught exception: " + toe.getMessage)
      }
    }
    if (ret == null) {
      ret = default(key)
      set(key, expire, ret)
    }
    ret
  }
}

trait EntityField extends Field {
  private val cacheClient = EntizerMemcachedClient
  var hashToDocFreq: HashMap[String, Int] = null
  var hashToIds: HashMap[String, Seq[ObjectId]] = null
  var numPhraseDuplicates: Int = 50
  var fracPhraseDuplicates: Double = 1.0
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
  var allowAllRootValues: Boolean = false

  def repository: MongoRepository

  def mentionColl: MongoCollection = repository.mentionColl

  def entityColl: MongoCollection = repository.collection(name + "_entity")

  def setMaxSegmentLength(maxSegLen: Int = Short.MaxValue.toInt) = {
    maxSegmentLength = maxSegLen
    this
  }

  def setAllowAllRootValues(flag: Boolean = false) = {
    allowAllRootValues = flag
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

  def setFractionPhraseDuplicates(fracPhraseDups: Double = 1.0) = {
    fracPhraseDuplicates = fracPhraseDups
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

  def cachePhrases() {
    idToPhrase = new EntityIdToPhraseProcessor(name, "phrase", entityColl).run().asInstanceOf[HashMap[ObjectId, Seq[String]]]
  }

  def clearPhrases() {
    idToPhrase = null
  }

  def cacheHashCodes() {
    idToHashCodes = new EntityIdToPhraseProcessor(name, "hashCodes", entityColl).run().asInstanceOf[HashMap[ObjectId, Seq[String]]]
  }

  def clearHashCodes() {
    idToHashCodes = null
  }

  def cacheMentionSegments() {
    idToMentionSegment = new EntityIdToMentionSegmentProcessor(name, entityColl).run().asInstanceOf[HashMap[ObjectId, MentionSegment]]
  }

  def clearMentionSegments() {
    idToMentionSegment = null
  }

  def cacheMentionToValues() {
    if (idToMentionSegment == null) cacheMentionSegments()
    mentionIdToValueIds = new HashMap[ObjectId, Seq[ObjectId]]
    for ((id, segment) <- idToMentionSegment) {
      mentionIdToValueIds(segment.mentionId) = mentionIdToValueIds.getOrElse(segment.mentionId, Seq.empty[ObjectId]) ++ Seq(id)
    }
  }

  def clearMentionToValues() {
    mentionIdToValueIds = null
  }

  def cacheSegmentToValues() {
    segmentToValues = new MentionSegmentToEntityValues
    val start = now()
    logger.info("")
    logger.info("Starting segmentToValues[field=" + name + "]")
    for (entityDbo <- entityColl.find() if entityDbo.isDefinedAt("segments")) {
      val entityValue = FieldValue(this, Some(entityDbo._id.get))
      val segmentDbos: Seq[DBObject] = entityDbo.get("segments").asInstanceOf[BasicDBList].map(_.asInstanceOf[DBObject])
      for (dbo <- segmentDbos) {
        val mentionId = dbo.get("mentionId").asInstanceOf[ObjectId]
        val begin = dbo.as[Int]("begin")
        val end = dbo.as[Int]("end")
        val segment = MentionSegment(mentionId, begin, end)
        // logger.info("segment[" + segment + "] -> value[" + entityValue.valueId + "]")
        segmentToValues(segment) = segmentToValues.getOrElse(segment, new HashSet[FieldValue]()) ++ Seq(entityValue)
      }
    }
    logger.info("Completed segmentToValues[field=" + name + "] in time=" + (now() - start) + " millis")
  }

  def clearSegmentToValues() {
    segmentToValues = null
  }

  def cacheAll() {
    cachePhrases()
    cacheHashCodes()
    cacheMentionSegments()
  }

  def clearAll() {
    clearPhrases()
    clearHashCodes()
    clearMentionSegments()
  }

  private def rehash() {
    // set up document frequency
    hashToDocFreq = new HashDocFreqProcessor(name, entityColl).run().asInstanceOf[HashMap[String, Int]]
    // set up inverted index
    val tmpHashToIds = new HashInvIndexProcessor(name, entityColl).run().asInstanceOf[HashMap[String, Seq[ObjectId]]]
    // remove hashes that link to more than maxfraction of entities
    val maxIds = (maxHashFraction * size).toInt
    var maxTmpIds = 0
    logger.info("pruning entities[%s] with #ids > %d (fraction=%.3f, size=%d)".format(name, maxIds, maxHashFraction, size))
    val emptyIds = Seq.empty[ObjectId]
    hashToIds = new HashMap[String, Seq[ObjectId]]
    for ((hash, ids) <- tmpHashToIds) {
      maxTmpIds = math.max(maxTmpIds, ids.size)
      hashToIds(hash) = {
        if (ids.size > maxIds) {
          logger.info("pruning hash='%s'".format(hash) + " [#ids=" + ids.size + "]")
          emptyIds
        } else {
          ids
        }
      }
    }
    logger.info("maximum hash to id mapping length=" + maxTmpIds)
  }

  private def initCanopies(debug: Boolean = false) {
    cacheAll()
    val startTime = now()
    logger.info("")
    logger.info("Starting canopyGeneration[field=" + name + "]")
    // first index all field hash codes
    var idx = new RAMDirectory
    val version = Version.LUCENE_35
    val analyzer = new StandardAnalyzer(version)
    try {
      var idxWriter = new IndexWriter(idx, new IndexWriterConfig(version, analyzer))
      for ((id, hashCodes) <- idToHashCodes) {
        val doc = new Document
        doc.add(new LuceneField("id", id.toByteArray))
        doc.add(new LuceneField("content", hashCodes.mkString(" "), LuceneField.Store.YES, LuceneField.Index.ANALYZED))
        idxWriter.addDocument(doc)
      }
      idxWriter.close(true)
      // create index searcher
      var searcher = new IndexSearcher(IndexReader.open(idx, true))
      // first identify centroids
      val segmentToEntityCoreValues = new EntityFieldMentionPossibleValueProcessor2(mentionColl, this, maxSimilarity,
        false, searcher, version, analyzer).run().asInstanceOf[MentionSegmentToEntityValues]
      // for each value select a small set of core values
      val possibleCoreValues = new HashSet[FieldValue]
      val rnd = new Random()
      // create all field values and shuffle for canopy
      val allValues = new ArrayBuffer[FieldValue] ++ idToHashCodes.keys.map(id => FieldValue(this, Some(id)))
      rnd.shuffle(allValues)
      val removedValues = new HashSet[FieldValue]
      for (canopyValue <- allValues if !removedValues(canopyValue)) {
        val canopySegment = idToMentionSegment(canopyValue.valueId.get)
        // canopy core
        val canopyCoreValues = segmentToEntityCoreValues.getOrElse(canopySegment, new HashSet[FieldValue])
        val numCoreValuesToTake = math.min(canopyCoreValues.size * fracPhraseDuplicates, numPhraseDuplicates).toInt
        val prunedCoreValues = canopyCoreValues.filter(!removedValues(_)).toSeq //.sortWith(_.hashCode() < _.hashCode())
          .take(numCoreValuesToTake - 1) ++ Seq(canopyValue)
        // logger.info("Canopy value: " + simpleValueString(canopyValue) + " core=" + canopyCoreValues.map(simpleValueString(_)).mkString("[", ", ", "]"))
        if (prunedCoreValues.size < canopyCoreValues.size) {
          logger.info("Filtering '%s' before=%d, after=%d".format(canopyValue.toString,
            canopyCoreValues.size, prunedCoreValues.size))
        }
        require(prunedCoreValues.size > 0, "#coreValues=0 for canopy value=" + canopyValue)
        possibleCoreValues ++= prunedCoreValues
        removedValues ++= canopyCoreValues
      }
      // create core index
      idx = new RAMDirectory
      idxWriter = new IndexWriter(idx, new IndexWriterConfig(version, analyzer))
      for (value <- possibleCoreValues) {
        val doc = new Document
        doc.add(new LuceneField("id", value.valueId.get.toByteArray))
        doc.add(new LuceneField("content", getValueHashes(value.valueId).mkString(" "), LuceneField.Store.YES, LuceneField.Index.ANALYZED))
        idxWriter.addDocument(doc)
      }
      idxWriter.close(true)
      searcher = new IndexSearcher(IndexReader.open(idx, true))
      // next for all segments find minimally similar to possible core values
      val segmentToAllValues = new EntityFieldMentionPossibleValueProcessor2(mentionColl, this, minSimilarity, true,
        searcher, version, analyzer, possibleCoreValues).run().asInstanceOf[MentionSegmentToEntityValues]
      logger.info("Total unique mentions: " + segmentToAllValues.keySet.map(_.mentionId).size)
      val numSegmentsTotal = segmentToAllValues.size
      var numSegmentsProcessed = 0
      val valueToAllSegments = new HashMap[FieldValue, Seq[MentionSegment]]
      for ((segment, segmentRelatedValues) <- segmentToAllValues) {
        for (value <- segmentRelatedValues if value.valueId.isDefined) {
          valueToAllSegments(value) = valueToAllSegments.getOrElse(value, Seq.empty[MentionSegment]) ++ Seq(segment)
        }

        numSegmentsProcessed += 1
        if (numSegmentsProcessed % 1000 == 0)
          logger.info("Processed segment -> values: " + numSegmentsProcessed + "/" + numSegmentsTotal)
      }
      // attach values to mention segments
      val numValuesTotal = valueToAllSegments.size
      var numValuesProcessed = 0
      val segmentStartTime = now()
      logger.info("")
      logger.info("Starting entityValueSegmentsAttacher[field=" + name + "]")
      for ((value, valueRelatedSegments) <- valueToAllSegments) {
        for (segment <- valueRelatedSegments) {
          val segmentDbo = MongoDBObject("mentionId" -> segment.mentionId, "begin" -> segment.begin, "end" -> segment.end)
          entityColl.update(MongoDBObject("_id" -> value.valueId.get), $push("segments" -> segmentDbo), false, false)
        }

        numValuesProcessed += 1
        if (numValuesProcessed % 1000 == 0)
          logger.info("Processed value -> segments: " + numValuesProcessed + "/" + numValuesTotal)
      }
      logger.info("Completed entityValueSegmentsAttacher[field=" + name + "] in time=" + (now() - segmentStartTime) + " millis")
    } catch {
      case ioe: IOException => ioe.printStackTrace()
    }
    logger.info("Completed canopyGeneration[field=" + name + "] in time=" + (now() - startTime) + " millis")
    // clear caches
    clearAll()
  }

  def init() = {
    entityColl.drop()
    // first add record fields to field collection
    new EntityInitializer(name, mentionColl, entityColl, maxSegmentLength, false, useFullSegment,
      useAllRecordSegments, useOracle, hashCodes).run()
    logger.info("#entities[%s]=%d".format(name, size))
    rehash()
    initCanopies()
    cacheMentionToValues()
    cacheSegmentToValues()
    this
  }

  def reinit() = {
    logger.info("#entities[%s]=%d".format(name, size))
    rehash()
    cacheMentionToValues()
    cacheSegmentToValues()
    this
  }

  def getHashDocumentFrequency = hashToDocFreq

  def getPossibleValues(mentionId: ObjectId, begin: Int, end: Int) = {
    (segmentToValues.getOrElse(MentionSegment(mentionId, begin, end), new HashSet[FieldValue]) ++
      Seq(FieldValue(this, None))).toSeq
  }

  def getValuePhrase(valueId: Option[ObjectId]) = {
    if (valueId.isDefined) {
      if (idToPhrase != null && idToPhrase.contains(valueId.get)) idToPhrase(valueId.get)
      else {
        cacheClient.entityGet[Seq[String]](name, valueId.get, "phrase", (key: String) => {
          val dbo = entityColl.findOneByID(valueId.get, MongoDBObject("phrase" -> 1)).get
          MongoHelper.getListAttr[String](dbo, "phrase")
        })
      }
    } else Seq.empty[String]
  }

  def getValueHashes(valueId: Option[ObjectId]) = {
    if (valueId.isDefined) {
      if (idToHashCodes != null && idToHashCodes.contains(valueId.get)) idToHashCodes(valueId.get)
      else {
        cacheClient.entityGet[Seq[String]](name, valueId.get, "hashCodes", (key: String) => {
          val dbo = entityColl.findOneByID(valueId.get, MongoDBObject("hashCodes" -> 1)).get
          MongoHelper.getListAttr[String](dbo, "hashCodes")
        })
      }
    } else Seq.empty[String]
  }

  def getValueMention(valueId: Option[ObjectId]) = {
    getValueMentionSegment(valueId).map(_.mentionId)
  }

  def getValueMentionSegment(valueId: Option[ObjectId]) = {
    if (valueId.isDefined) {
      if (idToMentionSegment != null && idToMentionSegment.contains(valueId.get)) Some(idToMentionSegment(valueId.get))
      else {
        val segment = cacheClient.entityGet[MentionSegment](name, valueId.get, "mentionSegment", (key: String) => {
          val dbo = entityColl.findOneByID(valueId.get, MongoDBObject("mentionId" -> 1, "begin" -> 1, "end" -> 1)).get
          val mentionId = dbo.get("mentionId").asInstanceOf[ObjectId]
          val begin = dbo.as[Int]("begin")
          val end = dbo.as[Int]("end")
          MentionSegment(mentionId, begin, end)
        })
        Some(segment)
      }
    } else None
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