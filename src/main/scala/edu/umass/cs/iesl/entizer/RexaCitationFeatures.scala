package edu.umass.cs.iesl.entizer

import collection.mutable.{HashSet, HashMap}
import org.apache.log4j.Logger
import io.Source
import java.io.{File, InputStream}
import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import cc.refectorie.user.kedarb.dynprog.data.CoraSgml2Owpl

/**
 * @author kedarb
 */

class RexaDict(val name: String, val toLC: Boolean = true) {
  val set = new HashSet[String]

  def add(s: String) {
    set += {
      if (toLC) s.toLowerCase else s
    }
  }

  def contains(s: String): Boolean = set.contains(if (toLC) s.toLowerCase else s)

  override def toString = name + " :: " + set.mkString("\n")
}

object RexaDict {
  val logger = Logger.getLogger(getClass.getSimpleName)

  def fromFile(filename: String, toLC: Boolean = true): RexaDict = {
    fromSource(filename, Source.fromFile(filename), toLC)
  }

  def fromResource(filename: String, toLC: Boolean = true): RexaDict = {
    fromSource(filename, Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(filename)), toLC)
  }

  def fromResourceOrFile(filename: String, toLC: Boolean = true): RexaDict = {
    try {
      val is: InputStream = getClass.getClassLoader.getResourceAsStream(filename)
      if (is != null) fromSource(filename, Source.fromInputStream(is), toLC)
      else if (new File(filename).exists) {
        logger.warn("Couldn't find file %s in classpath! Using relative path instead.".format(filename))
        fromSource(filename, Source.fromFile(filename), toLC)
      } else {
        logger.warn("Couldn't find file %s in classpath or relative path! Returning empty dict.".format(filename))
        new RexaDict(new File(filename).getName, toLC)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        new RexaDict(new File(filename).getName, toLC)
    }
  }

  def fromSource(filename: String, source: Source, toLC: Boolean = true): RexaDict = {
    val name = new File(filename).getName
    val dict = new RexaDict(name, toLC)
    for (line <- source.getLines()) dict.add(line)
    dict
  }
}

class RexaTrieDict(val toLC: Boolean = true) {
  val map = new HashMap[String, RexaTrieDict] {
    override def default(key: String) = {
      val trie = new RexaTrieDict(toLC);
      this(key) = trie;
      trie
    }
  }
  var isDone = false

  def add(a: Seq[String]) {
    var t = this
    forIndex(a.length, {
      k: Int =>
        val ak = if (toLC) a(k).toLowerCase else a(k)
        t = t.map(ak)
    })
    t.isDone = true
  }

  def str2seq(s: String): Seq[String] = CoraSgml2Owpl.Lexer.findAllIn(s).toSeq

  def add(s: String) {
    add(str2seq(s))
  }

  def contains(a: Seq[String]): Boolean = {
    var t = this
    forIndex(a.length, {
      k: Int =>
        val ak = if (toLC) a(k).toLowerCase else a(k)
        if (!t.map.contains(ak)) return false
        else t = t.map(ak)
    })
    t.isDone
  }

  def contains(s: String): Boolean = contains(str2seq(s))

  /**
   * Returns the end index
   */
  def endIndexOf(a: Seq[String], begin: Int): Int = {
    var t = this
    var end = begin
    while (end < a.length) {
      val ak = if (toLC) a(end).toLowerCase else a(end)
      if (!t.map.contains(ak)) return -1
      else {
        t = t.map(ak)
        if (t.isDone) return end
      }
      end += 1
    }
    if (t.isDone) a.length - 1
    else -1
  }
}

object RexaTrieDict {
  val logger = Logger.getLogger(getClass.getSimpleName)

  def fromFile(filename: String, toLC: Boolean = true): RexaTrieDict = {
    fromSource(Source.fromFile(filename), toLC)
  }

  def fromResource(filename: String, toLC: Boolean = true): RexaTrieDict = {
    fromSource(Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(filename)), toLC)
  }

  def fromResourceOrFile(filename: String, toLC: Boolean = true): RexaTrieDict = {
    try {
      val is: InputStream = getClass.getClassLoader.getResourceAsStream(filename)
      if (is != null) fromSource(Source.fromInputStream(is), toLC)
      else if (new File(filename).exists) {
        logger.warn("Couldn't find file %s in classpath! Using relative path instead.".format(filename))
        fromSource(Source.fromFile(filename), toLC)
      } else {
        logger.warn("Couldn't find file %s in classpath or relative path! Returning empty dict.".format(filename))
        new RexaTrieDict(toLC)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        new RexaTrieDict(toLC)
    }
  }

  def fromSource(source: Source, toLC: Boolean = true): RexaTrieDict = {
    val dict = new RexaTrieDict(toLC)
    try {
      for (line <- source.getLines()) dict.add(line)
    } catch {
      case e: Exception => {
        // e.printStackTrace()
        println("Error while reading trie source: " + e.getMessage)
      }
    }
    dict
  }
}

object RexaCitationFeatures {
  val prefixToFtrFns = new HashMap[String, String => Option[String]]
  val prefixToTrieLexicon = new HashMap[String, RexaTrieDict]
  val LEX_ROOT = "citations/lexicons/"

  def LexiconResource(name: String, filename: String, toLC: Boolean = true) {
    val dict = RexaDict.fromResourceOrFile(LEX_ROOT + filename, toLC)
    prefixToFtrFns("LEXICON=" + name) = {
      s: String =>
        val smod = (if (toLC) s.toLowerCase else s).replaceAll("[^A-Za-z0-9]+", " ").trim()
        if (smod.length > 3 && dict.contains(smod)) Some("") else None
    }
  }

  def trieLex(filename: String, toLC: Boolean = false): RexaTrieDict = {
    RexaTrieDict.fromResourceOrFile(LEX_ROOT + filename, toLC)
  }

  def regex(name: String, pattern: String) {
    prefixToFtrFns("REGEX=" + name) = {
      s: String => if (s.matches(pattern)) Some("") else None
    }
  }

  def RegexMatcher(name: String, pattern: String) {
    regex(name, pattern)
  }

  val CAPS = "[A-Z]"
  val ALPHA = "[A-Za-z]"
  val ALPHANUM = "[A-Za-z0-9]"
  val NUM = "[0-9]"
  val PUNC = "[,\\.;:?!()\"'`]"

  // define feature functions
  LexiconResource("DBLPTITLESTARTHIGH", "title.start.high", false)
  LexiconResource("DBLPTITLEHIGH", "title.high")
  LexiconResource("DBLPAUTHORFIRST", "author-first", false)
  LexiconResource("DBLPAUTHORLAST", "author-last", false)
  LexiconResource("CONFABBR", "conferences.abbr", false)
  LexiconResource("PLACES", "places")
  // add trie lexicons
  prefixToTrieLexicon += "TECH" -> trieLex("tech.txt")
  prefixToTrieLexicon += "JOURNAL" -> trieLex("journals")
  prefixToTrieLexicon += "CONFFULL" -> trieLex("conferences.full")
  prefixToTrieLexicon += "NOTEWORDS" -> trieLex("note-words.txt")
  prefixToTrieLexicon += "DBLPPUBLISHER" -> trieLex("publisher")

  LexiconResource("INSTITUTELEX", "institute-words.txt", false)
  LexiconResource("FirstHighest", "personname/ssdi.prfirsthighest")
  LexiconResource("FirstHigh", "personname/ssdi.prfirsthigh")
  LexiconResource("LastHighest", "personname/ssdi.prlasthighest")
  LexiconResource("LastHigh", "personname/ssdi.prlasthigh")
  LexiconResource("Honorific", "personname/honorifics")
  LexiconResource("NameSuffix", "personname/namesuffixes")
  LexiconResource("NameParticle", "personname/name-particles")
  LexiconResource("Nickname", "personname/nicknames")
  LexiconResource("Day", "days")
  LexiconResource("Month", "months")
  LexiconResource("StateAbbrev", "state_abbreviations")
  LexiconResource("Stopword", "stopwords")
  prefixToTrieLexicon += "University" -> trieLex("utexas/UNIVERSITIES")
  prefixToTrieLexicon += "State" -> trieLex("US-states")
  prefixToTrieLexicon += "Country" -> trieLex("countries")
  prefixToTrieLexicon += "CapitalCity" -> trieLex("country-capitals")

  RegexMatcher("CONTAINSDOTS", "[^\\.]*\\..*")
  RegexMatcher("CONTAINSCOMMA", ".*,.*")
  RegexMatcher("CONTAINSDASH", ALPHANUM + "+-" + ALPHANUM + "*")
  RegexMatcher("ACRO", "[A-Z][A-Z\\.]*\\.[A-Z\\.]*")

  RegexMatcher("URL1", "www\\..*|https?://.*|ftp\\..*|.*\\.edu/?.*")

  // patterns involving numbers
  RegexMatcher("PossiblePage", "[0-9]+\\s*[\\-{#]+\\s*[0-9]+")
  RegexMatcher("PossibleVol", "[0-9][0-9]?\\s*\\([0-9]+\\)")
  RegexMatcher("5+digit", "[0-9][0-9][0-9][0-9][0-9]+")
  RegexMatcher("HasDigit", ".*[0-9].*")
  RegexMatcher("AllDigits", "[0-9]+")

  RegexMatcher("ORDINAL1", "(?ii)[0-9]+(?:st|nd|rd|th)")
  RegexMatcher("ORDINAL2", ("(?ii)(?:"
    + "[Ff]irst|[Ss]econd|[Tt]hird|[Ff]ourth|[Ff]ifth|[Ss]ixth|[Ss]eventh|[Ee]ighth|[Nn]inth|[Tt]enth"
    + "|[Ee]leventh|[Tt]welfth|[Tt]hirteenth|[Ff]ourteenth|[Ff]ifteenth|[Ss]ixteenth"
    + "|[Ss]eventeenth|[Ee]ighteenth|[Nn]ineteenth"
    + "|[Tt]wentieth|[Tt]hirtieth|[Ff]ou?rtieth|[Ff]iftieth|[Ss]ixtieth|[Ss]eventieth"
    + "|[Ee]ightieth|[Nn]ine?tieth|[Tt]wentieth|[Hh]undredth"
    + ")"))

  // Punctuation
  RegexMatcher("Punc", PUNC)
  RegexMatcher("LeadQuote", "[\"'`]")
  RegexMatcher("EndQuote", "[\"'`][^s]?")
  RegexMatcher("MultiHyphen", "\\S*-\\S*-\\S*")
  RegexMatcher("ContainsPunc", "[\\-,\\:\\;]")
  RegexMatcher("StopPunc", "[\\!\\?\\.\"\']")

  // Character-based
  RegexMatcher("LONELYINITIAL", CAPS + "\\.")
  RegexMatcher("CAPLETTER", CAPS)
  RegexMatcher("ALLCAPS", CAPS + "+")

  // Field specific
  RegexMatcher("PossibleEditor", "(ed\\.|editor|editors|eds\\.)")
  RegexMatcher("PossiblePage", "(pp\\.|page|pages)")
  RegexMatcher("PossibleVol", "(no\\.|vol\\.?|volume)")
  RegexMatcher("PossibleInstitute", "(University|Universite|Universiteit|Univ\\.?|Dept\\.?|Institute" +
    "|Corporation|Department|Laboratory|Laboratories|Labs)")
}