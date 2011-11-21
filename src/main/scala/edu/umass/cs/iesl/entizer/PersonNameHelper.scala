package edu.umass.cs.iesl.entizer

import collection.mutable.{HashSet, HashMap}
import uk.ac.shef.wit.simmetrics.similaritymetrics._

/**
 * @author kedar
 */

trait StringSimilarityHelper {
  val JWINK = new JaroWinkler
  val LEVENSTEIN = new Levenshtein
  val QGRAMSIM = new QGramsDistance()
  val COSINESIM = new CosineSimilarity()
  val SMITHWATERMAN_AFFINE = new SmithWatermanGotohWindowedAffine()

  def quote(phr: Seq[String]) = phr.mkString("'", " ", "'")

  def getThresholdLevenshtein(_s: String, _t: String, threshold: Int = 3): Int = {
    val (s, t) = if (_s.length > _t.length) (_s, _t) else (_t, _s)
    val slen = s.length
    val tlen = t.length

    var prev = Array.fill[Int](tlen + 1)(Int.MaxValue)
    var curr = Array.fill[Int](tlen + 1)(Int.MaxValue)
    for (n <- 0 until math.min(tlen + 1, threshold + 1)) prev(n) = n

    for (row <- 1 until (slen + 1)) {
      curr(0) = row
      val min = math.min(tlen + 1, math.max(1, row - threshold))
      val max = math.min(tlen + 1, row + threshold + 1)

      if (min > 1) curr(min - 1) = Int.MaxValue
      for (col <- min until max) {
        curr(col) = if (s(row - 1) == t(col - 1)) prev(col - 1)
        else math.min(prev(col - 1), math.min(curr(col - 1), prev(col))) + 1
      }
      prev = curr
      curr = Array.fill[Int](tlen + 1)(Int.MaxValue)
    }

    prev(tlen)
  }

  def getApproxSetSimilarity(phr1: Seq[String], phr2: Seq[String],
                           tokenMatchThreshold: Double = 0.9, normalize: Boolean = true): Double = {
    val bag1 = new HashSet[String] ++ phr1
    val bag2 = new HashSet[String] ++ phr2
    val bag1Size = 1.0 * bag1.size
    val bag2Size = 1.0 * bag2.size
    var numIntersection = 0.0
    bag1.foreach(w => {
      if (bag2(w)) {
        numIntersection += 1
        bag2.remove(w)
      } else {
        var bestMatchTok: String = null
        var bestMatchScore = Double.NegativeInfinity
        bag2.foreach(ow => {
          val matchScore = SMITHWATERMAN_AFFINE.getSimilarity(w, ow)
          if (matchScore > bestMatchScore && matchScore >= tokenMatchThreshold) {
            bestMatchTok = ow
            bestMatchScore = matchScore
          }
        })
        if (bestMatchScore >= tokenMatchThreshold) {
          numIntersection += bestMatchScore
          bag2.remove(bestMatchTok)
        }
      }
    })
    val numUnion = bag1Size + bag2Size - numIntersection
    if (numUnion == 0) 0.0
    else numIntersection / numUnion
  }
}

object PersonNameHelper extends StringSimilarityHelper {
  private val SUFFIXES = Set(
    "JR", "Jr", "Junior",
    "SR", "Sr", "Senior",
    "II", "III", "IV", "VI", "VII", "VIII"
  )
  private val PERSON_NAME_REGEX = "^([a-zA-Z]+(?:\\.)?(?: ([a-zA-Z]+(?:\\.)?|-|[A-Z] ' [A-Za-z]+))*)$"

  private val lastnames = Seq(
    "Bellare", "Belare", "Bellary", "Belari", "Beelare", "Bellar",
    "Carbonell", "Carbonel", "Caronel", "Curbonell"
  )
  private val namePhrases = Seq(
    Seq("James", "O", "'", "Toole"),
    // new person
    Seq("Gray", ",", "P.", "M.", "D."),
    Seq("P.", "M.", "D.", "Gray", "``"),
    Seq("Peter", "M.", "D", "Gray", "."),
    Seq("Gray", ",", "P."),
    // new person
    Seq("Bassiliades", ",", "N.", ","),
    Seq("Basilades", ",", "N.", ","),
    // new person
    Seq("Ramarathnam", "Venkatesan", ","),
    Seq("R.", "A.", "Venkateshan", ","),
    Seq("R.", "Venkateshan"),
    // new person
    Seq("Lam", ",", "K.", ",", "T.", "Lee", ","),
    Seq("Kam", "-", "yiu", "Lam"),
    Seq("Kam", "-", "Yiu", "Lam"),
    // new person
    Seq("Alfred", "O.", "Hero", "III"),
    Seq("A", "O", "Hero"),
    Seq("Alfred", "Olyphant", "Hero"),
    // new person
    Seq("Williams", "Ludwell", "Harrison", "III", ",", ".", ":"),
    Seq("Williams", "L.", "Harrison", "."),
    Seq("W.", "L.", "Harrison", "."),
    // new person
    Seq("Li", "-", "Ling", "Chen"),
    Seq("L.", "-", "L.", "Chen"),
    // new person
    Seq("Phillip", "B.", "Gibbons"),
    Seq("GIBBONS", ",", "P.", ","),
    Seq("P.", "B.", "Gibbons", ","),
    // new person
    Seq("Cees", "H.", "M.", "van", "Kemenade"),
    Seq("C.", "H.", "M.", "van", "Kemnade", "."),
    // new person
    Seq("Vaclav", "Hlavac"),
    Seq("V.", "Hlavc", "."),
    // new person
    Seq("Bernard", "C.", "Y.", "Tan"),
    Seq("Tan", ",", "B.", "C.", "Y.", ","),
    // new person
    Seq("Jeff", "Tupper"),
    Seq("TUPPER", "J.", ":"),
    Seq("Jeff", "Tuper", "."),
    Seq("J.", "Tupper", ","),
    Seq("JeFF", "Tupper", "."),
    // new person
    Seq("Mihran", "Tuceryan", ","),
    Seq("M.", "Tucryan", ","),
    Seq("M.", "T", "uceryan", ","),
    // new person
    Seq("Ross", "Whitaker", ","),
    Seq("R.", "hitaker", ","),
    Seq("Whitaker", ",", "R.", ","),
    // new person
    Seq("Michel", "Raynal"),
    Seq("M.", "Raynal", ","),
    Seq("M.", "<author-middle", "/>", "Raynal", "."),
    // new person
    Seq("Michael", "O.", "Duff"),
    Seq("MO", "Duff", "."),
    Seq("Duff", ",", "M.", "O.")
  )

  val confusingNamePairs = Seq(
    Seq("Colin J. Fidge", "[8] C. Fidge ,"),
    Seq("Colin J. Fidge", "[8] C. Fidge , \""),
    Seq("Deborah G. Johnson", "Johnson , D."),
    Seq("Deborah G. Johnson", "Johnson , D. S."),
    Seq("Deborah G. Johnson", "Johnson , D. S. \" Computers"),
    Seq("Deborah G. Johnson", "Johnson , D. S. \" Computers and"),
    Seq("Tony S. H. Lee", "T. Lee ."),
    Seq("Laurent Regnier", "[15] Laurent Regnier .")
  )

  /**
   * Adapted from simmetrics since it does not expose this function
   */
  def soundex(lastname: String, len: Int = 8): String = {
    if (lastname.trim().length() == 0) ""
    else {
      // clip length to between 4 and 10
      val slen = math.min(10, math.max(len, 4))

      // clean up string
      var slast = lastname.toUpperCase.replaceAll("[^A-Z]", " ").replaceAll("\\s+", "")

      if (slast.length() == 0) ""
      else {
        // retain first character as-is
        val firstChar = slast(0)

        // truncate string
        slast = {
          if (slast.length() > (slen * 4) + 1) slast.substring(1, slen * 4)
          else slast.substring(1)
        }

        // classic soundex
        slast = slast.replaceAll("[AEIOUWH]", "0").replaceAll("[BPFV]", "1").replaceAll("[CSKGJQXZ]", "2")
          .replaceAll("[DT]", "3").replaceAll("[L]", "4").replaceAll("[MN]", "5").replaceAll("[R]", "6")

        // remove equal adjacent digits
        val buff = new StringBuilder
        var prevChar: Char = Char.MinValue
        for (i <- 0 until slast.length()) {
          val currChar = slast(i)
          if (currChar != prevChar) {
            buff.append(currChar)
            prevChar = currChar
          }
        }
        // replace first letter code, remove zeros
        slast = buff.toString().replaceAll("0", "")
        // pad zeros on the right
        slast += "000000000000000000"
        // add back first letter of word
        slast = firstChar + slast
        // return prefix
        slast.substring(0, slen)
      }
    }
  }

  // LAST, FIRST MIDDLE+ -> FIRST MIDDLE+ LAST
  private def doReorderWords(phrase: Seq[String]): Seq[String] = {
    val commaIndex = phrase.indexOf(",")
    if (commaIndex < 0) phrase
    else phrase.drop(commaIndex + 1) ++ phrase.take(commaIndex)
  }

  // only retain capitalized words as possible names (ignores "de Souza", "van den Berg")
  private def doIgnoreWords(phrase: Seq[String]): Seq[String] = {
    phrase.map(w => w.replaceAll("^[^A-Z]*([A-Z].*)", "$1")).filter(w => w.matches("^[A-Za-z].*"))
  }

  // trim punctuation and name suffixes at the end
  private def doTrimEnd(phrase: Seq[String]): Seq[String] = {
    if (phrase.lastOption.isDefined && (phrase.last.matches("^[:;\\.\"\\(']$") || SUFFIXES(phrase.last)))
      doTrimEnd(phrase.dropRight(1))
    else phrase
  }

  def normalizeName(phrase: Seq[String]): Seq[String] = {
    // doTrimEnd(doIgnoreWords(doReorderWords(doTrimEnd(phrase))))
    doTrimEnd(doReorderWords(doTrimEnd(phrase)))
  }

  def hashName(phrase: Seq[String]): Seq[String] = {
    import PhraseHash._
    val pruned_phrase = phrase.map(normalize(_)).filter(_.length() > 1)
    val word_hash = ngramWordHash(pruned_phrase, 1)
    val char_hash = ngramsCharHash(phrase, Seq(4))
    (word_hash ++ char_hash).toSeq
  }

  def matchesName(str: String): Boolean = str.matches(PERSON_NAME_REGEX)

  def matchesName(phrase: Seq[String], normalize: Boolean = true): Boolean = {
    if (phrase.contains("and") || phrase.contains("AND")) false
    else matchesName((if (normalize) normalizeName(phrase) else phrase).mkString(" "))
  }

  def featuresName(phrase: Seq[String]): Seq[String] = {
    val normPhrase = normalizeName(phrase)
    if (normPhrase.length >= 2) {
      val (fname, lname) = {
        if (normPhrase.last.matches("^[A-Z]\\..*")) normPhrase.last -> normPhrase.head
        else normPhrase.head -> normPhrase.last
      }
      val phoneticCode = fname.head + "-" + soundex(lname)
      // val fullCode = fname.head + "-" + lname.toUpperCase
      // Seq(phoneticCode, fullCode)
      Seq(phoneticCode)
    } else if (normPhrase.length == 1) {
      val lname = normPhrase.last
      val phoneticCode = soundex(lname)
      // val fullCode = lname.toUpperCase
      // Seq(phoneticCode, fullCode)
      Seq(phoneticCode)
    } else Seq.empty[String]
  }

  private def isNameSimilar(s1: String, s2: String) =
    JWINK.getSimilarity(s1, s2) >= 0.95 || getThresholdLevenshtein(s1, s2, 3) <= 1

  def isLastNameMatch(phr1: Seq[String], phr2: Seq[String], normalize: Boolean = true): Boolean = {
    // get last names
    val lname1Opt = (if (normalize) normalizeName(phr1) else phr1).lastOption.map(_.toLowerCase.replaceAll("[^a-z]+", ""))
    val lname2Opt = (if (normalize) normalizeName(phr2) else phr2).lastOption.map(_.toLowerCase.replaceAll("[^a-z]+", ""))

    if (!lname1Opt.isDefined || !lname2Opt.isDefined) false
    else {
      val lname1 = lname1Opt.get
      val lname2 = lname2Opt.get
      if (lname1.length() <= 3 || lname2.length() <= 3) lname1 == lname2
      else isNameSimilar(lname1, lname2)
    }
  }

  def isFirstNameMatch(phr1: Seq[String], phr2: Seq[String], normalize: Boolean = true): Boolean = {
    val fname1Opt = (if (normalize) normalizeName(phr1) else phr1).dropRight(1).headOption.map(_.toLowerCase.replaceAll("[^a-z]+", ""))
    val fname2Opt = (if (normalize) normalizeName(phr2) else phr2).dropRight(1).headOption.map(_.toLowerCase.replaceAll("[^a-z]+", ""))

    if (!fname1Opt.isDefined || !fname2Opt.isDefined) false
    else {
      val fname1 = fname1Opt.get
      val fname2 = fname2Opt.get
      if (fname1.length() == 1 && fname2.startsWith(fname1)) true
      else if (fname2.length() == 1 && fname1.startsWith(fname2)) true
      else if (fname1.length() <= 3 || fname2.length() <= 3) fname1 == fname2
      else isNameSimilar(fname1, fname2)
    }
  }

  def isMiddleNamesMatch(phr1: Seq[String], phr2: Seq[String], normalize: Boolean = true): Boolean = {
    val allmiddles1 = (if (normalize) normalizeName(phr1) else phr1).dropRight(1).drop(1).map(_.toLowerCase.replaceAll("[^a-z]+", ""))
    val allmiddles2 = (if (normalize) normalizeName(phr2) else phr2).dropRight(1).drop(1).map(_.toLowerCase.replaceAll("[^a-z]+", ""))
    for (i <- 0 until math.min(allmiddles1.length, allmiddles2.length)) {
      val mname1 = allmiddles1(i)
      val mname2 = allmiddles2(i)
      if (mname1.length() == 1 && mname2.startsWith(mname1)) {} // skip: initials match
      else if (mname2.length() == 1 && mname1.startsWith(mname2)) {} // skip: initials match
      else if ((mname1.length() <= 3 || mname2.length() <= 3) && mname1 != mname2) return false
      else if (isNameSimilar(mname1, mname2)) {} // skip: approx name match
      else return false
    }
    true
  }

  def isNameMatch(phr1: Seq[String], phr2: Seq[String], normalize: Boolean = true): Boolean = {
    val normphr1 = (if (normalize) normalizeName(phr1) else phr1)
    val normphr2 = (if (normalize) normalizeName(phr2) else phr2)
    if (!matchesName(normphr1, false)) false
    else if (!matchesName(normphr2, false)) false
    else isLastNameMatch(normphr1, normphr2, false) &&
      isFirstNameMatch(normphr1, normphr2, false) &&
      isMiddleNamesMatch(normphr1, normphr2, false)
  }

  val hashNameFn: Seq[String] => Seq[String] = hashName(_)

  def main(args: Array[String]) {
    println()
    lastnames.foreach(s => println(s + " => " + soundex(s)))
    println()
    val hash2names =
      namePhrases
        .map(phr => hashNameFn(phr).map(hash => hash -> phr))
        .flatMap(identity(_))
        .foldLeft(HashMap[String, Seq[Seq[String]]]())((m, v) => {
        m(v._1) = m.getOrElse(v._1, Seq()) ++ Seq(v._2)
        m
      })
    for ((hash, names) <- hash2names) {
      // println("hash: " + hash)
      for (i <- 0 until names.size; j <- (i + 1) until names.size) {
        val normi = normalizeName(names(i))
        val normj = normalizeName(names(j))
        println(quote(normi) + (if (isNameMatch(names(i), names(j))) " EQ " else " NEQ ") + quote(normj) + ":" +
          " lastEQ=" + isLastNameMatch(normi, normj) +
          " firstEQ=" + isFirstNameMatch(normi, normj) +
          " middleEQ=" + isMiddleNamesMatch(normi, normj))
      }
    }
    for (phr <- namePhrases) {
      if (!matchesName(phr)) println("NOTNAME: " + quote(phr) + " " + quote(normalizeName(phr)))
    }
    for (namePairs <- confusingNamePairs) {
      val name1 = namePairs(0).split(" ")
      val name2 = namePairs(1).split(" ")
      if (matchesName(name1) && matchesName(name2)) {
        println(quote(name1) + (if (isNameMatch(name1, name2)) " EQ " else " NEQ ") + quote(name2))
      } else {
        println("NOTNAMES: " + quote(name1) + " " + quote(normalizeName(name1)) + " || " +
          quote(name2) + " " + quote(normalizeName(name2)))
      }
    }
  }
}

object TitleHelper extends StringSimilarityHelper {
  def normalizeTitle(phrase: Seq[String]) =
    phrase.map(_.toLowerCase.replaceAll("[^a-z0-9]+", "")).filter(_.length() > 0)

  private def mkTitle(phrase: Seq[String], normalize: Boolean = true) =
    (if (normalize) normalizeTitle(phrase) else phrase).mkString(" ")
      .replaceAll("^\\s+", "").replaceAll("\\s+$", "").replaceAll("\\s+", " ")

  def getTitleSimilarity(phr1: Seq[String], phr2: Seq[String], normalize: Boolean = true): Double = {
    val title1 = mkTitle(phr1, normalize)
    val title2 = mkTitle(phr2, normalize)
    LEVENSTEIN.getSimilarity(title1, title2)
  }

  def isTitleSimilar(phr1: Seq[String], phr2: Seq[String], normalize: Boolean = true): Boolean = {
    if (phr1.head.matches("^\\p{Punct}*$") || phr2.head.matches("^\\p{Punct}*$")) false
    else getTitleSimilarity(phr1, phr2, normalize) >= 0.95
  }
}

object BooktitleHelper extends StringSimilarityHelper {
  val ORDINAL1 = "(?ii)[0-9]*(?:st|nd|rd|th)"
  val ORDINAL2 = "(?ii)(?:" +
    "first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth" +
    "|eleventh|twelfth|thirteenth|fourteenth|fifteenth|sixteenth|seventeenth" +
    "|eighteenth|nineteenth|twentieth|thirtieth|fou?rtieth|fiftieth|sixtieth|seventieth" +
    "|eightieth|nine?tieth|twentieth|hundredth" + ")"

  def normalizeBooktitle(phrase: Seq[String]) =
    phrase.map(_.toLowerCase.replaceAll("[^a-z0-9]+", "")).filter(s => {
      s.length() > 0 && !s.matches("(in|of|the|and|on|by|for|to|&)") &&
        !s.matches(ORDINAL1) && !s.matches(ORDINAL2)
    })

  private def mkBooktitle(phrase: Seq[String], normalize: Boolean = true) =
    (if (normalize) normalizeBooktitle(phrase) else phrase).mkString(" ")
      .replaceAll("^\\s+", "").replaceAll("\\s+$", "").replaceAll("\\s+", " ")

  def getBooktitleSimilarity(phr1: Seq[String], phr2: Seq[String], tokenMatchThreshold: Double = 0.9,
                             normalize: Boolean = true): Double = {
    getApproxSetSimilarity((if (normalize) normalizeBooktitle(phr1) else phr1),
      (if (normalize) normalizeBooktitle(phr2) else phr2), tokenMatchThreshold, normalize)
  }

  def isBooktitleSimilar(phr1: Seq[String], phr2: Seq[String], normalize: Boolean = true): Boolean = {
    if (phr1.head.matches("^\\p{Punct}*$") || phr2.head.matches("^\\p{Punct}*$")) false
    else getBooktitleSimilarity(phr1, phr2, 0.9, normalize) >= 0.95
  }
}

object JournalHelper extends StringSimilarityHelper {
  val confusingJournalPairs = Seq(
    "Acta Inf ." -> "Acta Informatics",
    "Artif . Intell ." -> "Artificial Intelligence",
    "IEEE Multimedia" -> "ACM MultiMedia",
    "IBM Systems Journal" -> "IBM Sys . J.",
    "Int . J. Approx . Reasoning" -> "International Journal on Approximate Reasoning",
    "Advanced Robotics" -> "Adv . Robot .",
    "Autonomous Robots" -> "Auton . Robots",
    "Ann . Software  Eng ." -> "Annals  of  Software  Engineering",
    "Annals  of  Mathematics and Artificial  Intelligence" -> "Ann . Math  . Artif . Intell  .",
    "Annals  of  Pure  and Applied Logic" -> "Ann . Pure  Appl  . Logic"
  )

  def normalizeJournal(phrase: Seq[String]) =
    phrase.map(_.toLowerCase.replaceAll("[^a-z0-9]+", "")).filter(s => {
      s.length() > 0 && !s.matches("(in|of|the|and|on|by|for|to|&)")
    })

  def getJournalSimilarity(phr1: Seq[String], phr2: Seq[String],
                           tokenMatchThreshold: Double = 0.9, normalize: Boolean = true): Double = {
    getApproxSetSimilarity((if (normalize) normalizeJournal(phr1) else phr1),
      (if (normalize) normalizeJournal(phr2) else phr2), tokenMatchThreshold, normalize)
  }

  def isJournalSimilar(phr1: Seq[String], phr2: Seq[String],
                       tokenMatchThreshold: Double = 0.9, normalize: Boolean = true): Boolean = {
    if (phr1.head.matches("^\\p{Punct}*$") || phr2.head.matches("^\\p{Punct}*$")) false
    else getApproxSetSimilarity((if (normalize) normalizeJournal(phr1) else phr1),
      (if (normalize) normalizeJournal(phr2) else phr2), tokenMatchThreshold, normalize) >= 0.95
  }

  def main(args: Array[String]) {
    for ((s1, s2) <- confusingJournalPairs) {
      val phr1 = s1.split(" ")
      val phr2 = s2.split(" ")
      println(s1 + " -> " + s2 + ": " + getJournalSimilarity(phr1, phr2))
    }
  }
}

object CitationHelper extends StringSimilarityHelper {
  def normalizeCitation(phrase: Seq[String]) =
    phrase.map(_.toLowerCase.replaceAll("[^a-z]+", "")).filter(s => {
      s.length() > 0
    })

  def getCitationSimilarity(phr1: Seq[String], phr2: Seq[String], normalize: Boolean = true): Double = {
    val citation1 = (if (normalize) normalizeCitation(phr1) else phr1).mkString(" ")
    val citation2 = (if (normalize) normalizeCitation(phr2) else phr2).mkString(" ")
    COSINESIM.getSimilarity(citation1, citation2)
  }

  def isCitationSimilar(phr1: Seq[String], phr2: Seq[String], normalize: Boolean = true): Boolean = {
    getCitationSimilarity(phr1, phr2, normalize) >= 0.7
  }
}