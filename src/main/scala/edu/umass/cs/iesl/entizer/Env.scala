package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._
import org.riedelcastro.nurupo.HasLogger
import collection.mutable.HashMap
import uk.ac.shef.wit.simmetrics.similaritymetrics.JaroWinkler

/**
 * @author kedar
 */

trait Env extends HasLogger {
  val JWINK = new JaroWinkler

  def repo: MongoRepository

  def mentions: MongoCollection

  def removeSubsegmentAligns(alignSegPred: AlignSegmentPredicate) {
    val segmentToValues = new HashMap[MentionSegment, Seq[FieldValueMentionSegment]]
    for (value <- alignSegPred) {
      segmentToValues(value.mentionSegment) =
        segmentToValues.getOrElse(value.mentionSegment, Seq.empty[FieldValueMentionSegment]) ++ Seq(value)
    }
    var numRemoved = 0
    for (segment <- segmentToValues.keys) {
      val begin = segment.begin
      val end = segment.end
      for (i <- begin until end; j <- (i + 1) to end if (j - i) < (end - begin)) {
        val subsegment = MentionSegment(segment.mentionId, i, j)
        if (segmentToValues.contains(subsegment)) {
          for (subvalue <- segmentToValues(subsegment)) {
            if (alignSegPred.remove(subvalue)) {
              logger.debug("Removing subsegment[" + subsegment + "] of segment[" + segment + "] value: " + subvalue)
              numRemoved += 1
            }
          }
        }
      }
    }
    logger.info("Deleted " + numRemoved + " (overlapping) alignments from " + alignSegPred.predicateName)
  }

  def isMentionPhraseApproxContainedInValue(fv: FieldValue, m: Mention, begin: Int, end: Int,
                                            transforms: Seq[(Seq[String], Seq[String])],
                                            simThreshold: Double = 0.9): Boolean = {
    if (!fv.valueId.isDefined) false
    else {
      def rmPunct(seq: Seq[String]) = seq.map(_.replaceAll("[^A-Za-z0-9]+", "")).filter(_.length() > 0)
      val mentionPhraseClean = rmPunct(m.words.slice(begin, end))
      val valuePhrase = fv.field.getValuePhrase(fv.valueId)
      def isContained(phrFrom: Seq[String], phrTo: Seq[String]): Boolean = {
        if (phrTo.length == 0) false
        else {
          val numTokens = phrTo.length
          val fromVec = new HashMap[String, Double]
          val toVec = new HashMap[String, Double]
          for (w <- phrFrom) fromVec(w) = fromVec.getOrElse(w, 0.0) + 1
          for (w <- phrTo) toVec(w) = toVec.getOrElse(w, 0.0) + 1
          var numIntersection = 0.0
          for (w <- toVec.keys) {
            if (fromVec.contains(w)) {
              numIntersection += math.min(fromVec(w), toVec(w))
              fromVec.remove(w)
            } else {
              var bestScore = 0.0
              var bestKey: String = null
              for (ow <- fromVec.keys) {
                val score = JWINK.getSimilarity(w, ow)
                if (score > bestScore) {
                  bestScore = score
                  bestKey = ow
                }
              }
              if (bestScore >= 0.9 && bestKey != null) {
                numIntersection += bestScore * math.min(toVec(w), fromVec(bestKey))
                fromVec.remove(bestKey)
              }
            }
          }
          if (numTokens == 0) false
          else (numIntersection / numTokens) >= simThreshold
        }
      }
      for (transformValuePhrase <- PhraseHash.transformedPhrases(valuePhrase, transforms)) {
        val transformValuePhraseClean = rmPunct(transformValuePhrase)
        if (isContained(transformValuePhraseClean, mentionPhraseClean)) {
          logger.info("phrase: " + m.words.slice(begin, end) + " value: " + valuePhrase)
          return true
        }
      }
      false
    }
  }

  def isMentionPhraseContainedInValue(fv: FieldValue, m: Mention, begin: Int, end: Int,
                                      transforms: Seq[(Seq[String], Seq[String])]): Boolean = {
    if (!fv.valueId.isDefined) false
    else {
      def rmPunct(seq: Seq[String]) = seq.map(_.replaceAll("[^A-Za-z0-9]+", "")).filter(_.length() > 0)
      val mentionPhraseClean = rmPunct(m.words.slice(begin, end))
      val valuePhrase = fv.field.getValuePhrase(fv.valueId)
      def isContained(phrFrom: Seq[String], phrTo: Seq[String]): Boolean = {
        if (phrTo.length == 0) false
        else if (phrFrom.length < phrTo.length) false
        else {
          val phrFromSet = phrFrom.toSet
          for (wto <- phrTo) {
            if (!phrFromSet(wto) && !phrFromSet.exists(wfrom => JWINK.getSimilarity(wfrom, wto) >= 0.9))
              return false
          }
          true
        }
      }
      for (transformValuePhrase <- PhraseHash.transformedPhrases(valuePhrase, transforms)) {
        val transformValuePhraseClean = rmPunct(transformValuePhrase)
        if (isContained(transformValuePhraseClean, mentionPhraseClean)) {
          // logger.info("phrase: " + m.words.slice(begin, end) + " value: " + valuePhrase)
          return true
        }
      }
      false
    }
  }

  def getMentionIds(query: DBObject = MongoDBObject()) =
    mentions.find(query, MongoDBObject()).map(_._id.get).toSeq
}

