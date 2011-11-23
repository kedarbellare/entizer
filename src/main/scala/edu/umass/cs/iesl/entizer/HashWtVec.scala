package edu.umass.cs.iesl.entizer

import collection.mutable.{ArrayBuffer, HashMap}
import optimization.projections.{BoundsProjection, SimplexProjection, Projection}
import com.mongodb.casbah.Imports._

/**
 * @author kedar
 */

object SumZeroProjection extends Projection {
  def samplePoint(p1: Int) = throw new RuntimeException("Sampling not implemented!")

  def perturbePoint(p1: Array[Double], p2: Int) = throw new RuntimeException("Perturbing not implemented!")

  def project(point: Array[Double]) {
    val sum = point.foldLeft(0.0)(_ + _)
    if (sum != 0) {
      val avg = sum / point.length
      for (i <- 0 until point.length) point(i) -= avg
    }
  }
}

object PositivityProjection extends BoundsProjection(0.0, Double.PositiveInfinity)

class Params {
  val dict = new HashMap[String, Int]
  val vectors = new ArrayBuffer[HashWtVec]

  def get(name: String, projection: Projection = null): HashWtVec = {
    if (!dict.contains(name)) {
      dict(name) = vectors.size
      val vec = new HashWtVec
      vec.projection = projection
      vectors += vec
    }
    vectors(dict(name))
  }

  def contains(name: String) = dict.contains(name)

  def increment(that: Params, scale: Double) {
    for (key <- that.keys) {
      val thatvec = that.get(key)
      val thisvec = this.get(key)
      if (thatvec.projection != null) thisvec.projection = thatvec.projection
      thisvec.increment(thatvec, scale)
    }
  }

  def dot(that: Params) = {
    var sum = 0.0
    for (key <- that.keys) sum += get(key).dot(that.get(key))
    sum
  }

  def keys = dict.keys

  def mult(scale: Double) {
    vectors.foreach(_.mult(scale))
  }

  def project() {
    vectors.foreach(_.project())
  }

  def numParams = vectors.foldLeft(0)(_ + _.size)

  def projectParams(a: Array[Double]) = {
    var offset = 0
    for (vec <- vectors) {
      vec.projectPoint(a, offset)
      offset += vec.size
    }
    a
  }

  def getParams(a: Array[Double]) = {
    var offset = 0
    for (vec <- vectors) {
      vec.getInArray(a, offset)
      offset += vec.size
    }
    a
  }

  def setParams(a: Array[Double]) {
    var offset = 0
    for (vec <- vectors) {
      vec.setFromArray(a, offset)
      offset += vec.size
    }
  }

  def copy(that: Params, deep: Boolean = false) {
    for ((name, index) <- that.dict) {
      dict += name -> index
    }
    for (thatvec <- that.vectors) {
      val thisvec = new HashWtVec
      thisvec.copy(thatvec, deep)
      vectors += thisvec
    }
  }

  def size = vectors.size

  override def toString = (for ((key, index) <- dict) yield
    "\n%s:\n%s".format(key.toString, vectors(index).toString)).mkString("\n")
}

class HashWtVec {
  val dict = new HashMap[String, Int]
  val weights = new ArrayBuffer[Double]
  var projection: Projection = null

  private def _lookup(key: String, defaultValue: Double = 0) {
    if (!dict.contains(key)) {
      dict(key) = weights.size
      weights += defaultValue
    }
  }

  def get(key: String, defaultValue: Double = 0): Double = {
    _lookup(key, defaultValue)
    weights(dict(key))
  }

  def contains(key: String) = dict.contains(key)

  def keys = dict.keys

  def increment(key: String, value: Double) {
    _lookup(key)
    weights(dict(key)) += value
  }

  def set(key: String, value: Double) {
    _lookup(key)
    weights(dict(key)) = value
  }

  def increment(keys: Seq[String], value: Double) {
    for (key <- keys) increment(key, value)
  }

  def increment(that: HashWtVec, scale: Double) {
    for (key <- that.keys) increment(key, that.get(key) * scale)
  }

  def dot(keys: Seq[String]) = {
    var sum = 0.0
    for (key <- keys) sum += get(key)
    sum
  }

  def dot(thatvec: HashWtVec) = {
    var sum = 0.0
    for (key <- thatvec.keys) sum += get(key) * thatvec.get(key)
    sum
  }

  def mult(scale: Double) {
    for (i <- 0 until size) weights(i) *= scale
  }

  def copy(that: HashWtVec, deep: Boolean = false) {
    projection = that.projection
    for ((key, index) <- that.dict) {
      dict += key -> index
    }
    for (wt <- that.weights) {
      weights += (if (deep) wt else 0.0)
    }
  }

  def project() {
    if (projection != null) {
      val b = weights.toArray
      projection.project(b)
      for (i <- 0 until size) weights(i) = b(i)
    }
  }

  def projectPoint(a: Array[Double], offset: Int) = {
    if (projection != null) {
      val b = a.slice(offset, offset + size)
      projection.project(b)
      for (i <- 0 until size) a(offset + i) = b(i)
    }
    a
  }

  def getInArray(a: Array[Double], offset: Int) = {
    weights.copyToArray(a, offset, size)
    a
  }

  def setFromArray(a: Array[Double], offset: Int) {
    for (i <- 0 until size) weights(i) = a(offset + i)
  }

  def size = weights.size

  override def toString = dict.toSeq.map(x => (x._1, weights(x._2))).filter(_._2 != 0).sortWith(_._2 > _._2)
    .map(x => "\t%s\t%.5f".format(x._1.toString, x._2)).mkString("\n")
}

object HashWtVecTest extends App {
  val wt = new HashWtVec
  wt.projection = new SimplexProjection(1.0)
  wt.increment(("name", "kedar").toString(), 0.8)
  wt.increment(("name", "bellare").toString(), 0.7)
  wt.increment(("name", "kedar").toString(), 0.3)
  wt.increment(("address", "kedar").toString(), 0.1)
  wt.project()
  println(wt.dict)
  println(wt.weights)
  val a = Array.fill[Double](wt.size)(0.0)
  wt.getInArray(a, 0)
  println(a.mkString(", "))
  println(wt.weights)
  wt.setFromArray(a, 0)
  println(wt.weights)
  wt.mult(0.3)
  println(wt.weights)

  println()

  val zerowt = new HashWtVec
  zerowt.projection = SumZeroProjection
  zerowt.increment(("name", "kedar").toString(), 0.8)
  zerowt.increment(("name", "bellare").toString(), 0.7)
  zerowt.increment(("name", "kedar").toString(), -0.3)
  zerowt.increment(("address", "kedar").toString(), -0.1)
  zerowt.project()
  println(zerowt.dict)
  println(zerowt.weights)
  val b = Array.fill[Double](zerowt.size)(0.0)
  zerowt.getInArray(b, 0)
  println(b.mkString(", "))
  println(zerowt.weights)
  zerowt.setFromArray(b, 0)
  println(zerowt.weights)
  zerowt.mult(0.3)
  println(zerowt.weights)

  val weightsColl = new MongoRepository("weights_test").collection("weights")
  weightsColl.ensureIndex(MongoDBObject("group" -> 1))
  weightsColl.ensureIndex(MongoDBObject("group" -> 1, "feature" -> 1))

  def storeConstraintParams(constraintParams: Params) {
    val startTime = System.currentTimeMillis()
    println("Starting storeConstraintParameters")
    for (group <- constraintParams.keys) {
      val wtvec = constraintParams.get(group)
      for (feat <- wtvec.keys) {
        val wt = wtvec.get(feat)
        weightsColl.update(MongoDBObject("group" -> group, "feature" -> feat), $set("weight" -> wt), true, false)
      }
    }
    println("Completed storeConstraintParameters in time=" + (System.currentTimeMillis() - startTime) + " millis")
  }

  def loadConstraintParams(constraintParams: Params) {
    val startTime = System.currentTimeMillis()
    println("Starting loadConstraintParameters")
    for (group <- constraintParams.keys) {
      val wtvec = constraintParams.get(group)
      // get all parameters related to the group
      val key = "constraint_weights_group=" + group
      var storedWts: Seq[(String, Double)] = null.asInstanceOf[Seq[(String, Double)]]
      try {
        storedWts = EntizerMemcachedClient.get(key).asInstanceOf[Seq[(String, Double)]]
      } catch {
        case toe: Exception => {}
      }
      if (storedWts == null) {
        val wtmap = new HashMap[String, Double]
        for (feat <- wtvec.keys) wtmap(feat) = wtvec.get(feat)
        for (dbo <- weightsColl.find(MongoDBObject("group" -> group))) {
          val feat = dbo.as[String]("feature")
          val wt = dbo.as[Double]("weight")
          wtmap(feat) = wt
        }
        storedWts = wtmap.toSeq
      }
      EntizerMemcachedClient.set(key, 3600, storedWts)
      for ((feat, wt) <- storedWts) wtvec.set(feat, wt)
    }
    println("Completed loadConstraintParameters in time=" + (System.currentTimeMillis() - startTime) + " millis")
  }
  
  EntizerMemcachedClient.flush()
  val tparams = new Params
  tparams.get("1").set("a", 0.1)
  tparams.get("1").set("b", 0.2)
  tparams.get("1").set("c", 0.3)
  tparams.get("2").set("a", 0.4)
  tparams.get("2").set("b", 0.5)
  storeConstraintParams(tparams)
  println("storing: " + tparams)

  val lparams = new Params
  lparams.get("1").set("c", -0.1)
  lparams.get("1").set("d", -0.2)
  lparams.get("3").set("a", 0.6)
  lparams.get("3").set("b", 0.7)
  println("loading into: " + lparams)
  loadConstraintParams(lparams)
  println("loaded: " + lparams)
  EntizerMemcachedClient.shutdown()
}