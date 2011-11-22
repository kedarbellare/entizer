package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._
import io.Source
import collection.mutable.{HashSet, HashMap}
import java.io.{PrintWriter, File}
import System.{currentTimeMillis => now}

/**
 * @author kedar
 */

class MentionFileLoader(val inputColl: MongoCollection, val filename: String, val isRecordColl: Boolean,
                        val debugEvery: Int = 1000) extends CollectionProcessor {
  val NULL_CLUSTER = "##NULL##"

  // indices
  inputColl.ensureIndex("source")
  inputColl.ensureIndex("isRecord")
  inputColl.ensureIndex("cluster")

  def name = "mentionLoader[file=" + filename + "]"

  def run() {
    preRun()

    var count = 0
    val lineIter = Source.fromFile(filename).getLines()
    while (lineIter.hasNext) {
      val builder = MongoDBObject.newBuilder
      val cluster = lineIter.next()
      if (cluster != NULL_CLUSTER) builder += "cluster" -> cluster
      builder += "isRecord" -> isRecordColl
      builder += "source" -> filename
      builder += "words" -> lineIter.next().split("\t")
      builder += "bioLabels" -> lineIter.next().split("\t")
      // empty line
      lineIter.next()
      inputColl += builder.result()
      count += 1
      if (count % debugEvery == 0)
        logger.info("Loaded mentions[isRecord=" + isRecordColl + "] count=" + count)
    }
    logger.info("Finished loading mentions[isRecord=" + isRecordColl + "] count=" + count)
  }
}

class MentionWebpageStorer(val inputColl: MongoCollection, val outputDirname: String, val root: FieldCollection,
                           val params: Params, val constraintParams: Params, val constraintFns: Seq[ConstraintFunction],
                           val maxRecords: Int = 1500, val maxTexts: Int = 1500) extends CollectionProcessor {
  new File(outputDirname).mkdirs()

  // field value to (mention tag, phrase)
  val entityToMentionValue = new HashMap[FieldValue, HashSet[(String, String)]]
  val pageToWriter = new HashMap[String, PrintWriter]

  def mentionsPage = "index.html"

  def getEntityPage(fieldValue: FieldValue) = fieldValue.field.name + ".html"

  def name = "mentions[name=" + inputColl.name + "] => webpage[dir=" + outputDirname + "]"

  def decode(mention: Mention) = {
    val ispec = SimpleInferSpec(trueSegmentInfer = mention.isRecord, bestUpdate = true, stepSize = 0)
    if (constraintParams == null) new DefaultSegmentationInferencer(root, mention, params, params, ispec)
    else new ConstrainedSegmentationInferencer(root, mention, params, params, constraintParams, constraintParams,
      constraintFns, ispec)
  }

  def writeTo(fname: String, value: String) {
    if (!pageToWriter.contains(fname)) {
      pageToWriter(fname) = new PrintWriter(outputDirname + "/" + fname)
      pageToWriter(fname).println("<html><title>test</title><body><ol>")
    }
    pageToWriter(fname).println(value)
    logger.debug(fname + " => " + value)
  }

  def optionIdToString(optId: Option[ObjectId]) = if (optId.isDefined) optId.get.toString else "unknown"

  def getMentionName(mentionId: ObjectId) = "mention_" + optionIdToString(Some(mentionId))

  def getMentionLink(mentionId: ObjectId) = mentionsPage + "#" + getMentionName(mentionId)

  def getEntityName(fieldValue: FieldValue) = fieldValue.field.name + "_" + optionIdToString(fieldValue.valueId)

  def getEntityLink(fieldValue: FieldValue) = getEntityPage(fieldValue) + "#" + getEntityName(fieldValue)

  def processMention(src: String, mention: Mention) {
    val mentionLink = getMentionLink(mention.id)
    val mentionPhrase = mention.words.mkString(" ")
    writeTo(mentionsPage, "<li><a name='" + getMentionName(mention.id) + "'><b>mention " + mention.id + "</b></a><br>")
    writeTo(mentionsPage, "<b>source</b>:&nbsp;" + src + "<br>")
    writeTo(mentionsPage, "<b>true cluster</b>:&nbsp;" +
      (if (mention.trueClusterOption.isDefined) mention.trueClusterOption.get else "unknown") + "<br>")
    writeTo(mentionsPage, "<b>" + (if (mention.isRecord) "record" else "text") + "</b>:&nbsp;" + mentionPhrase + "<br>")

    writeTo(mentionsPage, "<b>true segmentation</b>:&nbsp;")
    for (segment <- mention.trueWidget) {
      val lbl = segment.label
      val begin = segment.begin
      val end = segment.end
      val phrase = mention.words.slice(begin, end).mkString(" ")
      if (lbl == "O") writeTo(mentionsPage, phrase + "&nbsp;")
      else writeTo(mentionsPage, "<i>" + lbl + "</i>[" + phrase + "]&nbsp;")
    }
    writeTo(mentionsPage, "<br>")
    val inferencer = decode(mention)
    var recordEntity = FieldValue(root, None)
    writeTo(mentionsPage, "<b>predicted alignment+segmentation</b>:&nbsp;")
    for (alignSegment <- inferencer.bestFieldValueTextSegmentation) {
      val alignment = alignSegment.values
      recordEntity = alignment(0)
      val fieldEntity = alignment(1)
      val segment = alignSegment.segment
      val lbl = segment.label
      val begin = segment.begin
      val end = segment.end
      val fieldPhrase = mention.words.slice(begin, end).mkString(" ")
      entityToMentionValue(recordEntity) =
        entityToMentionValue.getOrElse(recordEntity, new HashSet[(String, String)]) ++ Seq(mentionLink -> mentionPhrase)
      if (fieldEntity.field.name == "O") {
        writeTo(mentionsPage, fieldPhrase + "&nbsp;")
      } else {
        entityToMentionValue(fieldEntity) =
          entityToMentionValue.getOrElse(fieldEntity, new HashSet[(String, String)]) ++ Seq(mentionLink -> fieldPhrase)
        writeTo(mentionsPage, "<a href='" + getEntityLink(fieldEntity) + "'>" + getEntityName(fieldEntity) + "</a>&nbsp;<i>" +
          lbl + "</i>[" + fieldPhrase + "]&nbsp;")
      }
    }
    println("MENTION_CLUSTER_OUTPUT\t" + mention.id + "\t" + mention.isRecord +
      "\t" + (if (mention.trueClusterOption.isDefined) mention.trueClusterOption.get.toString else "") +
      "\t" + (if (recordEntity.valueId.isDefined) recordEntity.valueId.get.toString else ""));
    writeTo(mentionsPage, "<br/>")
    writeTo(mentionsPage, "<b>predicted cluster</b>:&nbsp;<a href='" + getEntityLink(recordEntity) + "'>" + getEntityName(recordEntity) + "</a>")
    writeTo(mentionsPage, "</li><br>")
  }

  def run() {
    val start = now()

    var cursor = inputColl.find(MongoDBObject("isRecord" -> true)).limit(maxRecords)
    cursor.options = 16
    for (dbo <- cursor; mention = new Mention(dbo).setFeatures(dbo)) processMention(dbo.as[String]("source"), mention)
    cursor.close()

    cursor = inputColl.find(MongoDBObject("isRecord" -> false)).limit(maxTexts)
    cursor.options = 16
    for (dbo <- cursor; mention = new Mention(dbo).setFeatures(dbo)) processMention(dbo.as[String]("source"), mention)
    cursor.close()

    for ((fieldValue, mentionValues) <- entityToMentionValue) {
      val entityPage = getEntityPage(fieldValue)
      writeTo(entityPage, "<li><a name='" + getEntityName(fieldValue) + "'>" + getEntityName(fieldValue) + "</a>" +
        "&nbsp;<b>" + fieldValue.field.getValuePhrase(fieldValue.valueId).mkString(" ") + "</b><br>")
      writeTo(entityPage, "size=" + mentionValues.size + "<br>")
      var i = 0
      for (mentionValue <- mentionValues) {
        i += 1
        writeTo(entityPage, "<a href='" + mentionValue._1 + "'>mention " + i + "</a>&nbsp;" + mentionValue._2 + "<br>")
      }
      writeTo(entityPage, "</li><br>")
    }

    for (writer <- pageToWriter.values) {
      writer.println("</ol></body></html>")
      writer.close()
    }

    logger.info("Completed " + name + " in time=" + (now() - start) + " millis")
  }
}