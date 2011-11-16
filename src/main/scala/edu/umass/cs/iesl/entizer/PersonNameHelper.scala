package edu.umass.cs.iesl.entizer

import uk.ac.shef.wit.simmetrics.similaritymetrics._
import collection.mutable.HashMap

/**
 * @author kedar
 */

object PersonNameHelper {
  private val SUFFIXES = Set(
    "JR", "Jr", "Junior",
    "SR", "Sr", "Senior",
    "II", "III", "IV", "VI", "VII", "VIII"
  )
  private val lastnames = Seq(
    "Bellare", "Belare", "Bellary", "Belari", "Beelare", "Bellar",
    "Carbonell", "Carbonel", "Caronel", "Curbonell"
  )
  private val namePhrases = Seq(
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
    Seq("C.", "H.", "M.", "van", "Kemenade", "."),
    // new person
    Seq("Vaclav", "Hlavac"),
    Seq("V.", "Hlavc", "."),
    // new person
    Seq("Bernard", "C.", "Y.", "Tan"),
    Seq("Tan", ",", "B.", "C.", "Y.", ","),
    // new person
    Seq("Jeff", "Tupper"),
    Seq("TUPPER", "J.", ":"),
    Seq("Jeff", "Tupper", "."),
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
    if (phrase.lastOption.isDefined && (phrase.last.matches("^\\p{Punct}*$") || SUFFIXES(phrase.last)))
      doTrimEnd(phrase.dropRight(1))
    else phrase
  }

  def normalizeName(phrase: Seq[String]): Seq[String] = {
    doTrimEnd(doIgnoreWords(doReorderWords(doTrimEnd(phrase))))
  }

  def hashName(phrase: Seq[String]): Seq[String] = {
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

  val hashNameFn: Seq[String] => Seq[String] = hashName(_)

  def main(args: Array[String]) {
    println()
    lastnames.foreach(s => println(s + " => " + soundex(s)))
    println()
    val hash2names =
      namePhrases
        .map(phr => {
        val name = phr.mkString("'", " ", "'")
        hashNameFn(phr).map(hash => hash -> name)
      })
        .flatMap(identity(_))
        .foldLeft(HashMap[String, Seq[String]]())((m, v) => {
        m(v._1) = m.getOrElse(v._1, Seq.empty[String]) ++ Seq(v._2)
        m
      })
    println(hash2names.mkString("\n"))
  }
}