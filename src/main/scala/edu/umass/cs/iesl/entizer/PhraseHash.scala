package edu.umass.cs.iesl.entizer

import collection.mutable.{ArrayBuffer, HashSet}

/**
 * @author kedar
 */


object PhraseHash {
  val _noHash = new HashSet[String]

  def noHash(phrase: Seq[String]): HashSet[String] = _noHash

  private def normalize(s: String): String = {
    if (s.startsWith("$") && s.endsWith("$")) s
    else s.replaceAll("[^A-Za-z0-9]+", " ")
      .replaceAll("^\\s+", "")
      .replaceAll("\\s+$", "")
      .replaceAll("\\s+", " ")
      .toLowerCase
  }

  def ngramWordHash(orig_phrase: Seq[String], n: Int): HashSet[String] = {
    val grams = new HashSet[String]
    val buff = new StringBuffer()
    val phrase = orig_phrase.map(normalize(_)).filter(_.length() > 0)
    for (i <- (1 - n) until phrase.length) {
      buff.setLength(0)
      for (j <- i until (i + n)) {
        // jth word
        if (j < 0) {
          // buff.append("$begin_").append(j).append('$')
        }
        else if (j >= phrase.length) {
          // buff.append("$end_+").append(j - phrase.length + 1).append('$')
        }
        else {
          buff.append(phrase(j))
          buff.append('#')
        }
      }
      grams += buff.toString
    }
    grams
  }

  def ngramsWordHash(phrase: Seq[String], ns: Seq[Int]): HashSet[String] = {
    val grams = new HashSet[String]
    ns.foreach(n => {
      grams ++= ngramWordHash(phrase, n)
    })
    grams
  }

  def ngramCharHash(phrase: Seq[String], n: Int): HashSet[String] = {
    val grams = new HashSet[String]
    phrase.foreach(w => {
      grams ++= ngramWordHash(w.map(_.toString), n)
    })
    grams
  }

  def ngramCharHash(s: String, n: Int): HashSet[String] = {
    ngramCharHash(Seq(s), n)
  }

  def ngramsCharHash(phrase: Seq[String], ns: Seq[Int]): HashSet[String] = {
    val grams = new HashSet[String]
    ns.foreach(n => {
      grams ++= ngramCharHash(phrase, n)
    })
    grams
  }

  def ngramsCharHash(s: String, ns: Seq[Int]): HashSet[String] = {
    ngramsCharHash(Seq(s), ns)
  }

  def transformedPhrases(phrase: Seq[String], transforms: Seq[(Seq[String], Seq[String])]): Seq[Seq[String]] = {
    val buff = new ArrayBuffer[Seq[String]]
    buff += phrase
    for ((from, to) <- transforms) {
      val begin = phrase.indexOfSlice(from)
      if (begin >= 0) {
        val after = (phrase.take(begin) ++ to ++ phrase.drop(begin + from.length))
        // println("before[" + phrase.mkString(" ") + "] => after[" + after.mkString(" ") + "]")
        buff += after
      }
    }
    buff.toSeq
  }
}