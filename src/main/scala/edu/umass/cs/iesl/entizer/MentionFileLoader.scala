package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._
import io.Source

/**
 * @author kedar
 */


class MentionFileLoader(val inputColl: MongoCollection, val filename: String, val isRecordColl: Boolean,
                        val debugEvery: Int = 1000) extends CollectionProcessor {
  val NULL_CLUSTER = "##NULL##"

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
    inputColl.ensureIndex("source")
    inputColl.ensureIndex("isRecord")
    logger.info("Finished loading mentions[isRecord=" + isRecordColl + "] count=" + count)
  }
}