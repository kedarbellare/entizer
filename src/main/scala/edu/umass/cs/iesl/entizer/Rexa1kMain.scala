package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._
import io.Source
import java.io.PrintWriter
import collection.mutable.{ArrayBuffer, HashMap}

/**
 * @author kedar
 */

object Rexa1kRepo extends MongoRepository("rexa1k_test")

object RexaEnv extends Env {

  import RexaCitationFeatures._

  val repo = Rexa1kRepo
  val mentions = repo.mentionColl
  val evalQuery = MongoDBObject("isRecord" -> false, "source" -> "data/rexa/rexa_hlabeled_citations.txt")

  val DELIM_ONLY_PATT = "^\\p{Punct}+$"
  val INITIALS_PATT = "^[A-Z]\\p{Punct}+$"
  val SPECIAL_TOK_PATT = "^(and|AND|et\\.?|al\\.?|[Ee]d\\.|[Ee]ds\\.?|[Ee]ditors?|[Vv]ol\\.?|[Nn]o\\.?|pp\\.?|[Pp]ages)$"
  val BEGIN_PAREN_PATT = "^[\\(\\[].*"
  val END_PAREN_PATT = ".*[\\)\\]]$"
  val BEGIN_ALPHA_PATT = "^[A-Za-z].*$"
  val BEGIN_NUM_PATT = "^[0-9].*$"
  val END_ALPHA_PATT = "^.*[A-Za-z]$"
  val END_NUM_PATT = "^.*[0-9]$"

  val YEAR = "(19|20)\\d\\d[a-z]?"
  val REFMARKER = "\\[[A-Za-z]*\\d+\\]"
  val INITIALS = "[A-Z]\\."
  val MONTH = "(?:january|february|march|april|may|june|july|august|september|october|november|december|" +
    "jan|feb|apr|jun|jul|aug|sep|sept|oct|nov|dec)"
  val DOTW = "(?:mon|tues?|wed(?:nes)?|thurs?|fri|satu?r?|sun)(?:day)?"

  val rexaSchemaMap = Map(
    "B-author" -> "B-author", "I-author" -> "I-author",
    "B-title" -> "B-title", "I-title" -> "I-title",
    "B-booktitle" -> "B-booktitle", "I-booktitle" -> "I-booktitle",
    "B-journal" -> "B-journal", "I-journal" -> "I-journal",
    "B-date" -> "B-date", "I-date" -> "I-date",
    "B-editor" -> "B-editor", "I-editor" -> "I-editor",
    "B-publisher" -> "O", "I-publisher" -> "O",
    "B-institution" -> "O", "I-institution" -> "O",
    "B-location" -> "O", "I-location" -> "O",
    "B-pages" -> "O", "I-pages" -> "O",
    "B-series" -> "O", "I-series" -> "O",
    "B-tech" -> "O", "I-tech" -> "O",
    "B-thesis" -> "O", "I-thesis" -> "O",
    "B-volume" -> "O", "I-volume" -> "O")

  def simplify(s: String): String = {
    if (s.matches(YEAR)) "$year$"
    else if (s.matches(REFMARKER)) "$refmarker$"
    else if (s.matches(INITIALS)) "$initials$"
    else if (s.toLowerCase.matches(MONTH)) "$month$"
    else if (s.toLowerCase.matches(DOTW)) "$day$"
    else if (s.matches("\\(" + YEAR + "\\)")) "$yearbraces$"
    else s.replaceAll("\\d", "0").toLowerCase
  }

  def getFeatures(words: Array[String]) = {
    val lookAheadFeatures = Array.fill(words.length)(new ArrayBuffer[String])
    val basicFeatures = Array.fill(words.length)(Seq.empty[String])
    for (ip <- 0 until words.length) {
      val word = words(ip)
      val feats = new ArrayBuffer[String]
      val simplified = simplify(word)
      if (simplified.matches("^\\$.*\\$$")) {
        feats += "SIMPLIFIED=" + simplified
      }
      for ((key, featfn) <- prefixToFtrFns) {
        if (featfn(word).isDefined) feats += key
      }
      // match trie and add to look ahead features
      for ((key, lex) <- prefixToTrieLexicon) {
        val endip = lex.endIndexOf(words, ip)
        if (endip >= ip) {
          for (k <- ip to endip) {
            lookAheadFeatures(k) += "PHRASEMATCH=" + key
          }
        }
      }

      // add look-ahead features
      feats ++= lookAheadFeatures(ip)
      basicFeatures(ip) = feats.toSeq
    }
    val features = Array.fill(words.length)(ArrayBuffer[String]())
    for (ip <- 0 until words.length) {
      for (offset <- -2 to 2) {
        if (offset == 0)
          features(ip) ++= basicFeatures(ip)
        else if (ip + offset >= 0 && ip + offset < words.length)
          features(ip) ++= basicFeatures(ip + offset).map(s => "%s@%d".format(s, offset))
      }
    }
    // logger.info("\nwords: " + words.toSeq + "\nfeatures: " + features.toSeq.mkString("\n"))
    features.toSeq
  }

  def getPossibleEnds(words: Array[String]) = {
    val ends = Array.fill(words.length + 1)(false)
    // logger.info("words: " + words.mkString(" "))
    for (j <- 1 until ends.length) {
      ends(j) = {
        if (j == words.length) true
        else if (words(j - 1).matches(DELIM_ONLY_PATT)) true
        else if (words(j - 1).matches(INITIALS_PATT)) true // <s> Barry, P. </s> The ...
        else if (words(j - 1).matches(SPECIAL_TOK_PATT) || words(j).matches(SPECIAL_TOK_PATT)) true // <s>X</s> and <s>Y</s>
        else if (words(j - 1).matches(BEGIN_PAREN_PATT) || words(j).matches(BEGIN_PAREN_PATT)) true // <s>(1993)</s>
        else if (words(j - 1).matches(END_PAREN_PATT) || words(j).matches(END_PAREN_PATT)) true
        else if (words(j - 1).matches(END_ALPHA_PATT) && words(j).matches(BEGIN_NUM_PATT)) true // alpha -> num
        else if (words(j - 1).matches(END_NUM_PATT) && words(j).matches(BEGIN_ALPHA_PATT)) true // num -> alpha
        else false
      }
      // if (j < words.length && ends(j)) logger.info("end@" + j + ": " + words(j - 1))
    }
    ends
  }
}

object Rexa1kLoadRecordMentions extends MentionFileLoader(Rexa1kRepo.mentionColl, "data/rexa1k/rexa_records.txt.1000", true)

object Rexa1kLoadTextMentions extends MentionFileLoader(Rexa1kRepo.mentionColl, "data/rexa1k/rexa_citations.txt.1000.filtered", false)

object RexaLoadHlabeledTextMentions extends MentionFileLoader(Rexa1kRepo.mentionColl, "data/rexa/rexa_hlabeled_citations.txt", false)

// transforms the schema of mentions
object Rexa1kSchemaTransformer extends SchemaNormalizer(Rexa1kRepo.mentionColl, RexaEnv.rexaSchemaMap)

// computes the possible ends for text mentions
object Rexa1kAttachPossibleEnds extends PossibleEndsAttacher(Rexa1kRepo.mentionColl, "possibleEnds[rexa1k]") {
  def getPossibleEnds(words: Array[String]) = RexaEnv.getPossibleEnds(words)
}

// max length finder
object Rexa1kMaxLengths extends MaxLengthsProcessor(Rexa1kRepo.mentionColl, true)

// features attacher
object Rexa1kAttachFeatures extends FeaturesAttacher(Rexa1kRepo.mentionColl, "features[rexa1k]") {
  def getFeatures(words: Array[String]) = RexaEnv.getFeatures(words)
}

object Rexa1kInitMain extends App {
  Rexa1kRepo.clear()
  Rexa1kLoadRecordMentions.run()
  RexaLoadHlabeledTextMentions.run()
  // Rexa1kLoadTextMentions.run()
  Rexa1kSchemaTransformer.run()
  Rexa1kAttachPossibleEnds.run()
  Rexa1kAttachFeatures.run()
}

object Rexa1kMain extends App {

  import RexaEnv._

  val maxLengths = Rexa1kMaxLengths.run().asInstanceOf[HashMap[String, Int]]
  println("maxLengthMap=" + maxLengths)

  // create fields
  val otherField = SimpleField("O").setMaxSegmentLength(maxLengths("O")).init()
  val authorField = SimpleField("author").setMaxSegmentLength(maxLengths("author")).init()
  val editorField = SimpleField("editor").setMaxSegmentLength(maxLengths("editor")).init()
  val titleField = SimpleField("title").setMaxSegmentLength(maxLengths("title")).init()
  val booktitleField = SimpleField("booktitle").setMaxSegmentLength(maxLengths("booktitle")).init()
  val journalField = SimpleField("journal").setMaxSegmentLength(maxLengths("journal")).init()
  val dateField = SimpleField("date").setMaxSegmentLength(maxLengths("date")).init()

  // create record and add fields
  val citationRecord = SimpleRecord("citation").init()
  citationRecord
    .addField(otherField).addField(authorField).addField(titleField).addField(editorField).addField(booktitleField)
    .addField(journalField).addField(dateField)

  // learn from records only
  val useOracle = true
  val params = new SupervisedSegmentationOnlyLearner(mentions, citationRecord, useOracle).learn(50)
  logger.info("parameters: " + params)
  val evalName = "rexa1k-segmentation-only-uses-texts-" + useOracle
  val evalStats = new DefaultSegmentationEvaluator(evalName, mentions, params, citationRecord, true, evalQuery).run()
    .asInstanceOf[(Params, Option[PrintWriter], Option[PrintWriter])]._1
  TextSegmentationHelper.outputEval(evalName, evalStats, logger.info(_))
  // new MentionWebpageStorer(mentions, evalName, citationRecord, params, null, null).run()
}