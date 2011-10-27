package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._
import io.Source

/**
 * @author kedar
 */


class ClusterFileLoader(val inputColl: MongoCollection, val filename: String,
                        val debugEvery: Int = 1000) extends CollectionProcessor {
  def name = "clusterLoader[file=" + filename + "]"

  def run() {
    preRun()

    var count = 0
    val lineIter = Source.fromFile(filename).getLines()
    while (lineIter.hasNext) {
      val parts = lineIter.next().split("\t")
      val _id = new ObjectId(parts(0))
      val cluster = parts(1)
      inputColl.update(MongoDBObject("_id" -> _id), $set("cluster" -> cluster), upsert = false, multi = false)
      count += 1
      if (count % debugEvery == 0)
        logger.info("Updated cluster count=" + count)
    }
    logger.info("Finished updating cluster count=" + count)
    inputColl.ensureIndex("cluster")
  }
}