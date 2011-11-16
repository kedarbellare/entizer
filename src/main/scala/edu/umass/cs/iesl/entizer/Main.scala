package edu.umass.cs.iesl.entizer

import collection.mutable.HashMap

/**
 * @author kedar
 */

object Main extends App {
  val repo = new MongoRepository("mytest")
  repo.clear()

  // load records and texts
  new MentionFileLoader(repo.mentionColl, "data/rexa1k/rexa_records.txt.1000", true).run()
  new MentionFileLoader(repo.mentionColl, "data/rexa1k/rexa_citations.txt.1000", false).run()

  // normalize schema (if needed say B/I-booktitle -> B/I-venue, B/I-pages -> O, etc.)
  // author booktitle date editor institution journal location pages publisher series tech thesis title volume
  val schemaMappings = Map(
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
    "B-volume" -> "O", "I-volume" -> "O")
  new SchemaNormalizer(repo.mentionColl, schemaMappings).run()

  // attach possible ends
  new PossibleEndsAttacher(repo.mentionColl, "possibleEnds[mytest]") {
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
  }.run()

  // get maxlengths
  val maxLengths = new MaxLengthsProcessor(repo.mentionColl, true).run().asInstanceOf[HashMap[String, Int]]
  println("maxLengthMap=" + maxLengths)

  // initialize entity fields
  val authorEntColl = repo.collection("authorEntity")
  new EntityInitializer("author", repo.mentionColl, authorEntColl, maxLengths("author")).run()
  println("#authorEntities=" + authorEntColl.count)

  val hashCodesTitle: Seq[String] => Seq[String] = PhraseHash.ngramsWordHash(_, Seq(1, 2, 3)).toSeq
  val titleEntColl = repo.collection("titleEntity")
  new EntityInitializer("title", repo.mentionColl, titleEntColl, maxLengths("title")).run()
  println("#titleEntities=" + titleEntColl.count)

  val venueEntColl = repo.collection("venueEntity")
  new EntityInitializer("venue", repo.mentionColl, venueEntColl, maxLengths("venue"), true).run()
  println("#venueEntities=" + venueEntColl.count)

  // compute doc frequencies
  val docFreqTitleHashes = new HashDocFreqProcessor("title", titleEntColl).run().asInstanceOf[HashMap[String, Int]]
  println(docFreqTitleHashes.filter(_._2 >= 15).mkString("\n"))

  val docFreqAuthorHashes = new HashDocFreqProcessor("author", authorEntColl).run().asInstanceOf[HashMap[String, Int]]
  println(docFreqAuthorHashes.filter(_._2 >= 0).mkString("\n"))
}