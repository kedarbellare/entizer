package edu.umass.cs.iesl.entizer.rexa

import edu.umass.cs.iesl.entizer._

/**
 * @author kedar
 */

// initializes the rexa repository
object RexaRepo extends MongoRepository("rexa-entizer-scratch")

// loads the record mentions into repo
object RexaRecordsLoader extends MentionFileLoader(RexaRepo.mentionsColl, "data/rexa/rexa_records.txt", true, 10000)

// loads the citation mentions into repo
object RexaTextsLoader extends MentionFileLoader(RexaRepo.mentionsColl, "data/rexa/rexa_citations.txt", false, 10000)

// transforms the schema of mentions
object RexaSchemaTransformer extends SchemaNormalizer(RexaRepo.mentionsColl,
  Map(
    "B-author" -> "B-author", "I-author" -> "I-author",
    "B-booktitle" -> "B-venue", "I-booktitle" -> "I-venue",
    "B-date" -> "B-date", "I-date" -> "I-date",
    "B-editor" -> "O", "I-editor" -> "O",
    "B-institution" -> "O", "I-institution" -> "O",
    "B-journal" -> "B-venue", "I-journal" -> "I-venue",
    "B-location" -> "O", "I-location" -> "O",
    "B-pages" -> "O", "I-pages" -> "O",
    "B-publisher" -> "O", "I-publisher" -> "O",
    "B-series" -> "O", "I-series" -> "O",
    "B-tech" -> "O", "I-tech" -> "O",
    "B-thesis" -> "O", "I-thesis" -> "O",
    "B-title" -> "B-title", "I-title" -> "I-title",
    "B-volume" -> "O", "I-volume" -> "O"))

// computes the possible ends for text mentions
object RexaPossibleEndsAttacher extends PossibleEndsAttacher(RexaRepo.mentionsColl, "possibleEnds[rexa-scratch]") {
  val DELIM_ONLY_PATT = "^\\p{Punct}+$"
  val INITIALS_PATT = "^[A-Z]\\p{Punct}+$"
  val SPECIAL_TOK_PATT = "^(and|AND|et\\.?|al\\.?|[Ee]d\\.|[Ee]ds\\.?|[Ee]ditors?|[Vv]ol\\.?|[Nn]o\\.?|pp\\.?|[Pp]ages)$"
  val BEGIN_PAREN_PATT = "^[\\(\\[].*"
  val END_PAREN_PATT = ".*[\\)\\]]$"
  val BEGIN_ALPHA_PATT = "^[A-Za-z].*$"
  val BEGIN_NUM_PATT = "^[0-9].*$"
  val END_ALPHA_PATT = "^.*[A-Za-z]$"
  val END_NUM_PATT = "^.*[0-9]$"

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

// max length finder
object RexaMaxLengthsProcessor extends MaxLengthsProcessor(RexaRepo.mentionsColl, false)

object RexaMain extends App {
  RexaRepo.clear()
}