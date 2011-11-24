package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._
import collection.mutable.HashMap
import com.mongodb.casbah.Imports

/**
 * @author kedar
 */

abstract class PossibleEndsAttacher(val inputColl: MongoCollection, val name: String) extends ParallelCollectionProcessor {
  def inputJob = JobCenter.Job(select = MongoDBObject("words" -> 1))

  def getPossibleEnds(words: Array[String]): Array[Boolean]

  def process(dbo: DBObject, inputParams: Any, partialOutputParams: Any) {
    val _id: ObjectId = dbo._id.get
    val words = MongoHelper.getListAttr[String](dbo, "words").toArray
    val ends = getPossibleEnds(words)
    inputColl.update(MongoDBObject("_id" -> _id), $set("possibleEnds" -> ends), false, false)
  }
}

abstract class FeaturesAttacher(val inputColl: MongoCollection, val name: String) extends ParallelCollectionProcessor {
  def inputJob = JobCenter.Job(select = MongoDBObject("words" -> 1, "isRecord" -> 1))

  def getFeatures(isRecord: Boolean, words: Array[String]): Seq[Seq[String]]

  def process(dbo: DBObject, inputParams: Any, partialOutputParams: Any) {
    val _id: ObjectId = dbo._id.get
    val isRecord = dbo.as[Boolean]("isRecord")
    val words = MongoHelper.getListAttr[String](dbo, "words").toArray
    val features = getFeatures(isRecord, words)
    inputColl.update(MongoDBObject("_id" -> _id), $set("features" -> features), false, false)
  }
}

class FeaturesEraser(val inputColl: MongoCollection, val featuresType: String) extends ParallelCollectionProcessor {
  def name = "clearFeatures[name=" + inputColl.name + "][type=" + featuresType + "]"

  def inputJob = JobCenter.Job()

  def process(dbo: DBObject, inputParams: Any, partialOutputParams: Any) {
    inputColl.update(MongoDBObject("_id" -> dbo._id.get), $unset(featuresType), false, false)
  }
}

class SchemaNormalizer(val inputColl: MongoCollection, val schemaMappings: Map[String, String])
  extends ParallelCollectionProcessor {
  def name = "schemaNormalizer[transforms=" + schemaMappings + "]"

  // process all mentions
  def inputJob = JobCenter.Job(select = MongoDBObject("bioLabels" -> 1))

  // override def debugEvery = 1000

  def process(dbo: DBObject, inputParams: Any, partialOutputParams: Any) {
    val _id: ObjectId = dbo._id.get
    val labels = MongoHelper.getListAttr[String](dbo, "bioLabels").toArray
    for (i <- 0 until labels.length) {
      if (schemaMappings.contains(labels(i))) {
        labels(i) = schemaMappings(labels(i))
      }
    }
    inputColl.update(MongoDBObject("_id" -> _id), $set("bioLabels" -> labels), false, false)
  }
}

class FeaturesCounter(val inputColl: MongoCollection) extends ParallelCollectionProcessor {
  def name = "featuresCounter"

  def inputJob = JobCenter.Job(select = MongoDBObject("features" -> 1))

  override def newOutputParams(isMaster: Boolean = false) = new HashMap[String, Int]

  override def merge(outputParams: Any, partialOutputParams: Any) {
    val outputCounts = outputParams.asInstanceOf[HashMap[String, Int]]
    for ((feat, count) <- partialOutputParams.asInstanceOf[HashMap[String, Int]]) {
      outputCounts(feat) = outputCounts.getOrElse(feat, 0) + count
    }
  }

  def process(dbo: DBObject, inputParams: Any, partialOutputParams: Any) {
    val features = MongoHelper.getListOfListAttr[String](dbo, "features")
    val partialCounts = partialOutputParams.asInstanceOf[HashMap[String, Int]]
    for (fvec <- features; feat <- fvec) {
      partialCounts(feat) = partialCounts.getOrElse(feat, 0) + 1
    }
  }
}

class FeaturesPruner(val inputColl: MongoCollection, val featCounts: HashMap[String, Int], val minCount: Int)
  extends ParallelCollectionProcessor {
  def name = "featuresPruner"

  def inputJob = JobCenter.Job(select = MongoDBObject("features" -> 1))

  def process(dbo: DBObject, inputParams: Any, partialOutputParams: Any) {
    val features = MongoHelper.getListOfListAttr[String](dbo, "features")
    val prunedFeatures = features.map(_.filter(feat => featCounts.contains(feat) && featCounts(feat) >= minCount))
    inputColl.update(MongoDBObject("_id" -> dbo._id.get), $set("features" -> prunedFeatures), false, false)
  }
}