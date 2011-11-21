package edu.umass.cs.iesl.entizer

import collection.mutable.ArrayBuffer

/**
 * @author kedar
 */

case class TextSegment(label: String, begin: Int, end: Int) {
  override def toString = "%s[%d, %d)".format(label, begin, end)
}

class TextSegmentation extends ArrayBuffer[TextSegment] {
  override def toString() = this.mkString(" ")
}

object TextSegmentationHelper {
  def getLabelFromBIO(bioLabel: String) = if (bioLabel == "O") bioLabel else bioLabel.substring(2)

  def getTextSegmentationFromBIO(bioLabels: Seq[String]): TextSegmentation = {
    val segmentation = new TextSegmentation
    if (bioLabels.size > 0) {
      var segmentLabel = getLabelFromBIO(bioLabels(0))
      var segmentBegin = 0
      var segmentEnd = segmentBegin + 1
      def addSegment(position: Int, update: Boolean = true) {
        segmentation += TextSegment(segmentLabel, segmentBegin, segmentEnd)
        if (update) {
          segmentLabel = getLabelFromBIO(bioLabels(position))
          segmentBegin = position
          segmentEnd = segmentBegin + 1
        }
      }
      for (i <- 1 until bioLabels.size) {
        val currLabel = bioLabels(i)
        if (currLabel == "O") {
          if (segmentLabel == "O") segmentEnd += 1
          else addSegment(i)
        } else if (currLabel.startsWith("B-")) {
          addSegment(i)
        } else if (currLabel.startsWith("I-")) {
          segmentEnd += 1
        }
      }
      addSegment(bioLabels.size, false)
    }
    segmentation
  }

  def adjustSegmentation(words: Seq[String], pred: TextSegmentation,
                         isOtherWord: String => Boolean = _.matches("^[^A-Za-z0-9]*$")): TextSegmentation = {
    val adjpred = new TextSegmentation
    for (segment <- pred) {
      val mod_begin = {
        var i = segment.begin
        while (i < segment.end && isOtherWord(words(i))) i += 1
        i
      }
      if (mod_begin == segment.end) {
        // whole phrase is punctuations
        adjpred += TextSegment("O", segment.begin, segment.end)
      } else {
        if (mod_begin > segment.begin) {
          // prefix is punctuation
          adjpred += TextSegment("O", segment.begin, mod_begin)
        }
        val mod_end = {
          var i = segment.end - 1
          while (i >= mod_begin && isOtherWord(words(i))) i -= 1
          i + 1
        }
        if (mod_end == segment.end) {
          // rest is valid
          adjpred += TextSegment(segment.label, mod_begin, segment.end)
        } else {
          // suffix is punctuation
          adjpred += TextSegment(segment.label, mod_begin, mod_end)
          adjpred += TextSegment("O", mod_end, segment.end)
        }
      }
    }
    adjpred
  }

  def toWhatsWrong(words: Seq[String], segmentation: TextSegmentation): String = {
    val buff = new StringBuilder
    buff.append(">>\n>words\n")
    for (i <- 0 until words.length) {
      buff.append(i).append('\t').append('"').append(words(i)).append('"').append('\n')
    }
    buff.append("\n>labels\n")
    for (segment <- segmentation if segment.label != "O") {
      buff.append(segment.begin).append('\t').append(segment.end - 1).append('\t')
        .append('"').append(segment.label).append('"').append('\n')
    }
    buff.append('\n')
    buff.toString()
  }

  def updateEval(truth: TextSegmentation, pred: TextSegmentation, stats: Params) {
    for (segment <- truth if segment.label != "O") {
      stats.get(("segment" -> "OVERALL").toString()).increment("true", 1)
      stats.get(("segment"-> segment.label).toString()).increment("true", 1)
      for (i <- segment.begin until segment.end) {
        stats.get(("token" -> "OVERALL").toString()).increment("true", 1)
        stats.get(("token" -> segment.label).toString()).increment("true", 1)
        val predlbl = pred.filter(seg => i >= seg.begin && i < seg.end).head.label
        if (segment.label == predlbl) {
          stats.get(("token" -> "OVERALL").toString()).increment("correct", 1)
          stats.get(("token" -> segment.label).toString()).increment("correct", 1)
        }
      }
      if (pred.contains(segment)) {
        stats.get(("segment" -> "OVERALL").toString()).increment("correct", 1)
        stats.get(("segment" -> segment.label).toString()).increment("correct", 1)
      }
    }
    for (segment <- pred if segment.label != "O") {
      stats.get(("segment" -> "OVERALL").toString()).increment("pred", 1)
      stats.get(("segment" -> segment.label).toString()).increment("pred", 1)
      for (i <- segment.begin until segment.end) {
        stats.get(("token" -> "OVERALL").toString()).increment("pred", 1)
        stats.get(("token" -> segment.label).toString()).increment("pred", 1)
      }
    }
  }

  def outputEval(name: String, stats: Params, puts: (String) => Any) {
    for (lbl <- stats.keys) {
      val evals = stats.get(lbl)
      val correctCount = evals.get("correct")
      val trueCount = evals.get("true")
      val predCount = evals.get("pred")
      val pr: Double = if (predCount == 0) 0 else correctCount / predCount
      val re: Double = if (trueCount == 0) 0 else correctCount / trueCount
      val f1: Double = if (pr == 0 && re == 0) 0 else (2 * pr * re) / (pr + re)
      val evalStr = name + " accuracy: " + lbl +
        " precision=%.4f".format(pr) + " recall=%.4f".format(re) + " F1=%.4f".format(f1) +
        " correct=%.0f".format(correctCount) + " pred=%.0f".format(predCount) + " true=%.0f".format(trueCount)
      puts(evalStr)
    }
  }
}