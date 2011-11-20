package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._
import java.io.PrintWriter
import collection.mutable.{ArrayBuffer, HashMap}
import org.riedelcastro.nurupo.HasLogger

/**
 * @author kedar
 */

object Rexa1kRepo extends MongoRepository("rexa1k_test")

object RexaRepo extends MongoRepository("rexa_test")

trait ARexaEnv extends Env {

  import RexaCitationFeatures._

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
    "B-date" -> "O", "I-date" -> "O",
    "B-editor" -> "O", "I-editor" -> "O",
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

  def getFeatures(isRecord: Boolean, words: Array[String]) = {
    val lookAheadFeatures = Array.fill(words.length)(new ArrayBuffer[String])
    val basicFeatures = Array.fill(words.length)(Seq.empty[String])
    for (ip <- 0 until words.length) {
      val word = words(ip)
      val feats = new ArrayBuffer[String]
      val simplified = simplify(word)
      if (simplified.matches("^\\$.*\\$$")) {
        feats += "SIMPLIFIED=" + simplified
      } else if (simplified.matches(".*[a-z0-9].*")) {
        feats += "SIMPLIFIED=" + simplified.replaceAll("[^a-z0-9]+", "")
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
      for (offset <- -3 to 3) {
        if (offset == 0)
          features(ip) ++= basicFeatures(ip)
        else if (ip + offset >= 0 && ip + offset < words.length && !isRecord)
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

  def hashAuthorField(phrase: Seq[String]) = PersonNameHelper.hashName(phrase)

  def hashTitleField(phrase: Seq[String]) = PhraseHash.ngramWordHash(phrase, 1).toSeq

  def hashVenueField(phrase: Seq[String]) =
    PhraseHash.ngramWordHash(phrase.map(simplify(_)).filter(!_.matches("^(\\$.*\\$|[\\d\\p{Punct}]*)$")), 1).toSeq

  def hashCitationRecord(phrase: Seq[String]) = PhraseHash.ngramWordHash(phrase.map(simplify(_)), 1).toSeq

  def rexaName: String

  // field parameters
  def numAuthorDups: Int

  def maxAuthorHashFraction: Double

  def minAuthorSim: Double

  def maxAuthorSim: Double

  def numTitleDups: Int

  def maxTitleHashFraction: Double

  def minTitleSim: Double

  def maxTitleSim: Double

  def numBooktitleDups: Int

  def maxBooktitleHashFraction: Double

  def minBooktitleSim: Double

  def maxBooktitleSim: Double

  def numJournalDups: Int

  def maxJournalHashFraction: Double

  def minJournalSim: Double

  def maxJournalSim: Double

  def numCitationDups: Int

  def maxCitationHashFraction: Double

  def minCitationSim: Double

  def maxCitationSim: Double
}

object Rexa1kEnv extends ARexaEnv {
  val repo = Rexa1kRepo
  val mentions = repo.mentionColl
  val rexaName = "rexa1k"

  // field parameters
  val numAuthorDups = 20
  val maxAuthorHashFraction = 0.1
  val minAuthorSim = 0.4
  val maxAuthorSim = 0.9

  val numTitleDups = 1
  val maxTitleHashFraction = 0.05
  val minTitleSim = 0.8
  val maxTitleSim = 0.9

  val numBooktitleDups = 1
  val maxBooktitleHashFraction = 0.2
  val minBooktitleSim = 0.7
  val maxBooktitleSim = 0.9

  val numJournalDups = 1
  val maxJournalHashFraction = 0.2
  val minJournalSim = 0.7
  val maxJournalSim = 0.9

  val numCitationDups = 1
  val maxCitationHashFraction = 0.05
  val minCitationSim = 0.6
  val maxCitationSim = 0.8
}

object RexaEnv extends ARexaEnv {
  val repo = RexaRepo
  val mentions = repo.mentionColl
  val rexaName = "rexa"

  // field parameters
  val numAuthorDups = 20
  val maxAuthorHashFraction = 0.1
  val minAuthorSim = 0.4
  val maxAuthorSim = 0.9

  val numTitleDups = 1
  val maxTitleHashFraction = 0.01
  val minTitleSim = 0.8
  val maxTitleSim = 0.9

  val numBooktitleDups = 1
  val maxBooktitleHashFraction = 0.1
  val minBooktitleSim = 0.7
  val maxBooktitleSim = 0.9

  val numJournalDups = 1
  val maxJournalHashFraction = 0.1
  val minJournalSim = 0.7
  val maxJournalSim = 0.9

  val numCitationDups = 1
  val maxCitationHashFraction = 0.01
  val minCitationSim = 0.6
  val maxCitationSim = 0.8
}

object Rexa1kLoadRecordMentions extends MentionFileLoader(Rexa1kRepo.mentionColl, "data/rexa1k/rexa_records.txt.1000", true)

object RexaLoadRecordMentions extends MentionFileLoader(RexaRepo.mentionColl, "data/rexa/rexa_records.txt.filtered", true)

object Rexa1kLoadTextMentions extends MentionFileLoader(Rexa1kRepo.mentionColl, "data/rexa1k/rexa_citations.txt.1000.filtered", false)

object RexaLoadTextMentions extends MentionFileLoader(RexaRepo.mentionColl, "data/rexa/rexa_citations.txt.filtered", false)

object Rexa1kLoadHlabeledTextMentions extends MentionFileLoader(Rexa1kRepo.mentionColl, "data/rexa/rexa_hlabeled_citations.txt", false)

object RexaLoadHlabeledTextMentions extends MentionFileLoader(RexaRepo.mentionColl, "data/rexa/rexa_hlabeled_citations.txt", false)

// transforms the schema of mentions
object Rexa1kSchemaTransformer extends SchemaNormalizer(Rexa1kRepo.mentionColl, Rexa1kEnv.rexaSchemaMap)

object RexaSchemaTransformer extends SchemaNormalizer(RexaRepo.mentionColl, RexaEnv.rexaSchemaMap)

// computes the possible ends for text mentions
object Rexa1kAttachPossibleEnds extends PossibleEndsAttacher(Rexa1kRepo.mentionColl, "possibleEnds[rexa1k]") {
  def getPossibleEnds(words: Array[String]) = Rexa1kEnv.getPossibleEnds(words)
}

object RexaAttachPossibleEnds extends PossibleEndsAttacher(RexaRepo.mentionColl, "possibleEnds[rexa]") {
  def getPossibleEnds(words: Array[String]) = RexaEnv.getPossibleEnds(words)
}

// max length finder
object Rexa1kMaxLengths extends MaxLengthsProcessor(Rexa1kRepo.mentionColl, true)

object RexaMaxLengths extends MaxLengthsProcessor(RexaRepo.mentionColl, true)

// features attacher
object Rexa1kAttachFeatures extends FeaturesAttacher(Rexa1kRepo.mentionColl, "features[rexa1k]") {
  def getFeatures(isRecord: Boolean, words: Array[String]) = Rexa1kEnv.getFeatures(isRecord, words)
}

object RexaAttachFeatures extends FeaturesAttacher(RexaRepo.mentionColl, "features[rexa]") {
  def getFeatures(isRecord: Boolean, words: Array[String]) = RexaEnv.getFeatures(isRecord, words)
}

object Rexa1kInitMain extends App {
  Rexa1kRepo.clear()
  Rexa1kLoadRecordMentions.run()
  Rexa1kLoadHlabeledTextMentions.run()
  Rexa1kLoadTextMentions.run()
  Rexa1kSchemaTransformer.run()
  Rexa1kAttachPossibleEnds.run()
  Rexa1kAttachFeatures.run()
}

object RexaInitMain extends App {
  RexaRepo.clear()
  RexaLoadRecordMentions.run()
  RexaLoadHlabeledTextMentions.run()
  RexaLoadTextMentions.run()
  RexaSchemaTransformer.run()
  RexaAttachPossibleEnds.run()
  RexaAttachFeatures.run()
}

class ARexaPruneFeaturesMain(val mentions: MongoCollection) {
  // prune features that are not common
  val featureCounts = new FeaturesCounter(mentions).run().asInstanceOf[HashMap[String, Int]]
  val threshold = 1000
  println("Pruning #features=" + featureCounts.filter(_._2 < threshold).size)
  println("Total #features=" + featureCounts.size)
  new FeaturesPruner(mentions, featureCounts, threshold).run()
}

object Rexa1kPruneFeaturesMain extends ARexaPruneFeaturesMain(Rexa1kEnv.mentions) with App

object RexaPruneFeaturesMain extends ARexaPruneFeaturesMain(RexaEnv.mentions) with App

class ARexaInitEntitiesMain(val env: ARexaEnv) {

  EntityMemcachedClient.flush()
  val maxLengths = new MaxLengthsProcessor(env.mentions, true).run().asInstanceOf[HashMap[String, Int]]
  println("maxLengthMap=" + maxLengths)

  val repo = env.repo

  // create field entities
  val numMentions = repo.mentionColl.count.toInt
  new SimpleEntityField("author", repo)
    .setMaxSegmentLength(maxLengths("author")).setHashCodes(env.hashAuthorField(_))
    .setPhraseDuplicates(env.numAuthorDups).setMaxHashFraction(env.maxAuthorHashFraction)
    .setSimilarities(env.minAuthorSim, env.maxAuthorSim).init()
  new SimpleEntityField("title", repo)
    .setMaxSegmentLength(maxLengths("title")).setHashCodes(env.hashTitleField(_))
    .setPhraseDuplicates(env.numTitleDups).setMaxHashFraction(env.maxTitleHashFraction)
    .setSimilarities(env.minTitleSim, env.maxTitleSim).init()
  new SimpleEntityField("booktitle", repo)
    .setMaxSegmentLength(maxLengths("booktitle")).setHashCodes(env.hashVenueField(_))
    .setPhraseDuplicates(env.numBooktitleDups).setMaxHashFraction(env.maxBooktitleHashFraction)
    .setSimilarities(env.minBooktitleSim, env.maxBooktitleSim).setAllowAllRootValues(true).init()
  new SimpleEntityField("journal", repo)
    .setMaxSegmentLength(maxLengths("journal")).setHashCodes(env.hashVenueField(_))
    .setPhraseDuplicates(env.numJournalDups).setMaxHashFraction(env.maxJournalHashFraction)
    .setSimilarities(env.minJournalSim, env.maxJournalSim).setAllowAllRootValues(true).init()

  // create record entity
  new SimpleEntityRecord("citation", repo, false)
    .setHashCodes(env.hashCitationRecord(_)).setMaxHashFraction(env.maxCitationHashFraction)
    .setPhraseDuplicates(env.numCitationDups).setSimilarities(env.minCitationSim, env.maxCitationSim)
    .init()

  EntityMemcachedClient.shutdown()
}

object Rexa1kInitEntitiesMain extends ARexaInitEntitiesMain(Rexa1kEnv) with App

object RexaInitEntitiesMain extends ARexaInitEntitiesMain(RexaEnv) with App

class Rexa1kBasicMain(val useOracle: Boolean) extends HasLogger {

  import Rexa1kEnv._

  EntityMemcachedClient.flush()
  val maxLengths = Rexa1kMaxLengths.run().asInstanceOf[HashMap[String, Int]]
  println("maxLengthMap=" + maxLengths)

  // create fields
  val otherField = SimpleField("O").setMaxSegmentLength(maxLengths("O")).init()
  val authorField = SimpleField("author").setMaxSegmentLength(maxLengths("author")).init()
  val titleField = SimpleField("title").setMaxSegmentLength(maxLengths("title")).init()
  val booktitleField = SimpleField("booktitle").setMaxSegmentLength(maxLengths("booktitle")).init()
  val journalField = SimpleField("journal").setMaxSegmentLength(maxLengths("journal")).init()

  // create record and add fields
  val useRecordClustering = false
  val citationRecord = SimpleRecord("citation").init()
  citationRecord
    .addField(otherField).addField(authorField).addField(titleField).addField(booktitleField).addField(journalField)

  val params = new SupervisedSegmentationOnlyLearner(mentions, citationRecord, useOracle).learn(50)
  logger.info("parameters: " + params)
  val evalName = "rexa1k-segmentation-only-uses-texts-" + useOracle
  val evalStats = new DefaultSegmentationEvaluator(evalName, mentions, params, citationRecord, true, evalQuery).run()
    .asInstanceOf[(Params, Option[PrintWriter], Option[PrintWriter])]._1
  TextSegmentationHelper.outputEval(evalName, evalStats, logger.info(_))
  EntityMemcachedClient.shutdown()
}

object Rexa1kRecordOracleMain extends Rexa1kBasicMain(false) with App

object Rexa1kOracleMain extends Rexa1kBasicMain(true) with App

class ARexaCheckNameEquality(val env: ARexaEnv) extends HasLogger {

  import PersonNameHelper.{quote, isNameMatch, matchesName}

  val repo = env.repo
  val mentions = env.mentions

  val maxLengths = new MaxLengthsProcessor(mentions, true).run().asInstanceOf[HashMap[String, Int]]
  println("maxLengthMap=" + maxLengths)

  // create fields
  val authorField = new SimpleEntityField("author", repo)
    .setMaxSegmentLength(maxLengths("author")).setHashCodes(env.hashAuthorField(_))
    .setPhraseDuplicates(env.numAuthorDups).setMaxHashFraction(env.maxAuthorHashFraction)
    .setSimilarities(env.minAuthorSim, env.maxAuthorSim).reinit().asInstanceOf[SimpleEntityField]
  authorField.cacheAll()

  for (dbo <- mentions.find(); mention = new Mention(dbo)) {
    val allSegments =
      if (mention.isRecord) mention.allTrueSegments()
      else mention.possibleSegments(authorField.name, maxLengths("author"))
    for (segment <- allSegments) {
      val mentionPhrase = mention.words.slice(segment.begin, segment.end)
      for (value <- authorField.getPossibleValues(mention.id, segment.begin, segment.end)
           if value.valueId.isDefined) {
        val valuePhrase = authorField.getValuePhrase(value.valueId)
        if (matchesName(mentionPhrase) && matchesName(valuePhrase)) {
          val isMatch = if (isNameMatch(valuePhrase, mentionPhrase)) "match" else "nonmatch"
          logger.info(isMatch + "[isRecord=" + mention.isRecord + "]: " + quote(valuePhrase) + " <=> " + quote(mentionPhrase))
        }
      }
    }
  }
}

object Rexa1kCheckNameEquality extends ARexaCheckNameEquality(Rexa1kEnv) with App

object RexaCheckNameEquality extends ARexaCheckNameEquality(RexaEnv) with App

class ARexaCheckTitleEquality(val env: ARexaEnv) extends HasLogger {

  import TitleHelper.{quote, isTitleSimilar, getTitleSimilarity}

  val repo = env.repo
  val mentions = env.mentions

  val maxLengths = new MaxLengthsProcessor(mentions, true).run().asInstanceOf[HashMap[String, Int]]
  println("maxLengthMap=" + maxLengths)

  // create fields
  val titleField = new SimpleEntityField("title", repo)
    .setMaxSegmentLength(maxLengths("title")).setHashCodes(env.hashTitleField(_))
    .setPhraseDuplicates(env.numTitleDups).setMaxHashFraction(env.maxTitleHashFraction)
    .setSimilarities(env.minTitleSim, env.maxTitleSim).reinit().asInstanceOf[SimpleEntityField]
  titleField.cacheAll()

  for (dbo <- mentions.find(); mention = new Mention(dbo)) {
    val allSegments =
      if (mention.isRecord) mention.allTrueSegments()
      else mention.possibleSegments(titleField.name, maxLengths("title"))
    for (segment <- allSegments) {
      val mentionPhrase = mention.words.slice(segment.begin, segment.end)
      for (value <- titleField.getPossibleValues(mention.id, segment.begin, segment.end)
           if value.valueId.isDefined) {
        val valuePhrase = titleField.getValuePhrase(value.valueId)
        if (isTitleSimilar(valuePhrase, mentionPhrase)) {
          logger.info("")
          logger.info("similar[isRecord=" + mention.isRecord + ", similarity=" +
            getTitleSimilarity(valuePhrase, mentionPhrase) + "]")
          logger.info(quote(valuePhrase))
          logger.info(quote(mentionPhrase))
        }
      }
    }
  }
}

object Rexa1kCheckTitleEquality extends ARexaCheckTitleEquality(Rexa1kEnv) with App

object RexaCheckTitleEquality extends ARexaCheckTitleEquality(RexaEnv) with App

class ARexaCheckBooktitleEquality(val env: ARexaEnv) extends HasLogger {

  import BooktitleHelper.{quote, getBooktitleSimilarity, isBooktitleSimilar}

  val repo = env.repo
  val mentions = env.mentions

  val maxLengths = new MaxLengthsProcessor(mentions, true).run().asInstanceOf[HashMap[String, Int]]
  println("maxLengthMap=" + maxLengths)

  val booktitleField = new SimpleEntityField("booktitle", repo)
    .setMaxSegmentLength(maxLengths("booktitle")).setHashCodes(env.hashVenueField(_))
    .setPhraseDuplicates(env.numBooktitleDups).setMaxHashFraction(env.maxBooktitleHashFraction)
    .setSimilarities(env.minBooktitleSim, env.maxBooktitleSim)
    .setAllowAllRootValues(true).reinit().asInstanceOf[SimpleEntityField]
  booktitleField.cacheAll()

  for (dbo <- mentions.find(); mention = new Mention(dbo)) {
    val allSegments =
      if (mention.isRecord) mention.allTrueSegments()
      else mention.possibleSegments(booktitleField.name, maxLengths("booktitle"))
    for (segment <- allSegments) {
      val mentionPhrase = mention.words.slice(segment.begin, segment.end)
      for (value <- booktitleField.getPossibleValues(mention.id, segment.begin, segment.end)
           if value.valueId.isDefined) {
        val valuePhrase = booktitleField.getValuePhrase(value.valueId)
        if (isBooktitleSimilar(valuePhrase, mentionPhrase)) {
          logger.info("")
          logger.info("similar[isRecord=" + mention.isRecord + ", similarity=" +
            getBooktitleSimilarity(valuePhrase, mentionPhrase) + "]")
          logger.info(quote(valuePhrase))
          logger.info(quote(mentionPhrase))
        }
      }
    }
  }
}

object Rexa1kCheckBooktitleEquality extends ARexaCheckBooktitleEquality(Rexa1kEnv) with App

object RexaCheckBooktitleEquality extends ARexaCheckBooktitleEquality(RexaEnv) with App

class ARexaCheckJournalEquality(val env: ARexaEnv) extends HasLogger {

  import JournalHelper.{quote, getJournalSimilarity, isJournalSimilar}

  val repo = env.repo
  val mentions = env.mentions

  val maxLengths = new MaxLengthsProcessor(mentions, true).run().asInstanceOf[HashMap[String, Int]]
  println("maxLengthMap=" + maxLengths)

  val journalField = new SimpleEntityField("journal", repo)
    .setMaxSegmentLength(maxLengths("journal")).setHashCodes(env.hashVenueField(_))
    .setPhraseDuplicates(env.numJournalDups).setMaxHashFraction(env.maxJournalHashFraction)
    .setSimilarities(env.minJournalSim, env.maxJournalSim)
    .setAllowAllRootValues(true).reinit().asInstanceOf[SimpleEntityField]
  journalField.cacheAll()

  for (dbo <- mentions.find(); mention = new Mention(dbo)) {
    val allSegments =
      if (mention.isRecord) mention.allTrueSegments()
      else mention.possibleSegments(journalField.name, maxLengths("journal"))
    for (segment <- allSegments) {
      val mentionPhrase = mention.words.slice(segment.begin, segment.end)
      for (value <- journalField.getPossibleValues(mention.id, segment.begin, segment.end)
           if value.valueId.isDefined) {
        val valuePhrase = journalField.getValuePhrase(value.valueId)
        if (!mention.isRecord && isJournalSimilar(valuePhrase, mentionPhrase)) {
          logger.info("")
          logger.info("similar[isRecord=" + mention.isRecord + ", similarity=" +
            getJournalSimilarity(mentionPhrase, valuePhrase) + "]")
          logger.info(quote(valuePhrase))
          logger.info(quote(mentionPhrase))
        }
      }
    }
  }
}

object Rexa1kCheckJournalEquality extends ARexaCheckJournalEquality(Rexa1kEnv) with App

object RexaCheckJournalEquality extends ARexaCheckJournalEquality(RexaEnv) with App

class ARexaCheckCitationEquality(val env: ARexaEnv) extends HasLogger {

  import CitationHelper.{quote, isCitationSimilar, getCitationSimilarity}

  val repo = env.repo
  val mentions = env.mentions

  val maxLengths = new MaxLengthsProcessor(mentions, true).run().asInstanceOf[HashMap[String, Int]]
  println("maxLengthMap=" + maxLengths)

  val citationRecord = new SimpleEntityRecord("citation", repo, false)
    .setHashCodes(env.hashCitationRecord(_)).setMaxHashFraction(env.maxCitationHashFraction)
    .setPhraseDuplicates(env.numCitationDups).setSimilarities(env.minCitationSim, env.maxCitationSim)
    .reinit().asInstanceOf[SimpleEntityRecord]
  citationRecord.cacheAll()

  for (dbo <- mentions.find(); mention = new Mention(dbo)) {
    val allSegments = mention.fullSegments(citationRecord.name)
    for (segment <- allSegments) {
      val mentionPhrase = mention.words.slice(segment.begin, segment.end)
      for (value <- citationRecord.getPossibleValues(mention.id, segment.begin, segment.end)
           if value.valueId.isDefined) {
        val valuePhrase = citationRecord.getValuePhrase(value.valueId)
        if (!mention.isRecord && isCitationSimilar(valuePhrase, mentionPhrase)) {
          logger.info("")
          logger.info("similar[isRecord=" + mention.isRecord + ", similarity=" +
            getCitationSimilarity(mentionPhrase, valuePhrase) + "]")
          logger.info(quote(valuePhrase))
          logger.info(quote(mentionPhrase))
        }
      }
    }
  }
}

object Rexa1kCheckCitationEquality extends ARexaCheckCitationEquality(Rexa1kEnv) with App

object RexaCheckCitationEquality extends ARexaCheckCitationEquality(RexaEnv) with App

class RexaRunMain(val env: ARexaEnv) extends HasLogger {
  EntityMemcachedClient.flush()
  val maxLengths = new MaxLengthsProcessor(env.mentions, true).run().asInstanceOf[HashMap[String, Int]]
  println("maxLengthMap=" + maxLengths)

  val repo = env.repo
  val mentions = env.mentions

  // create fields
  val otherField = SimpleField("O").setMaxSegmentLength(maxLengths("O")).init()
  val authorField = new SimpleEntityField("author", repo)
    .setMaxSegmentLength(maxLengths("author")).setHashCodes(env.hashAuthorField(_))
    .setPhraseDuplicates(env.numAuthorDups).setMaxHashFraction(env.maxAuthorHashFraction)
    .setSimilarities(env.minAuthorSim, env.maxAuthorSim).reinit().asInstanceOf[SimpleEntityField]
  val titleField = new SimpleEntityField("title", repo)
    .setMaxSegmentLength(maxLengths("title")).setHashCodes(env.hashTitleField(_))
    .setPhraseDuplicates(env.numTitleDups).setMaxHashFraction(env.maxTitleHashFraction)
    .setSimilarities(env.minTitleSim, env.maxTitleSim).reinit().asInstanceOf[SimpleEntityField]
  val booktitleField = new SimpleEntityField("booktitle", repo)
    .setMaxSegmentLength(maxLengths("booktitle")).setHashCodes(env.hashVenueField(_))
    .setPhraseDuplicates(env.numBooktitleDups).setMaxHashFraction(env.maxBooktitleHashFraction)
    .setSimilarities(env.minBooktitleSim, env.maxBooktitleSim)
    .setAllowAllRootValues(true).reinit().asInstanceOf[SimpleEntityField]
  val journalField = new SimpleEntityField("journal", repo)
    .setMaxSegmentLength(maxLengths("journal")).setHashCodes(env.hashVenueField(_))
    .setPhraseDuplicates(env.numJournalDups).setMaxHashFraction(env.maxJournalHashFraction)
    .setSimilarities(env.minJournalSim, env.maxJournalSim)
    .setAllowAllRootValues(true).reinit().asInstanceOf[SimpleEntityField]

  // create record entity
  // TODO: check that allow all root values has correct behavior
  val citationRecord = new SimpleEntityRecord("citation", repo, false)
    .setHashCodes(env.hashCitationRecord(_)).setMaxHashFraction(env.maxCitationHashFraction) //.setAllowAllRootValues(true)
    .setPhraseDuplicates(env.numCitationDups).setSimilarities(env.minCitationSim, env.maxCitationSim)
    .reinit().asInstanceOf[SimpleEntityRecord]
  citationRecord
    .addField(otherField).addField(authorField).addField(titleField).addField(booktitleField).addField(journalField)

  // initialize constraints
  val constraintFns = new ArrayBuffer[ConstraintFunction]

  // most author mentions are similar to entity values
  authorField.cacheAll()
  val authorMentionMatchesPredicate = new FieldMentionAlignPredicateProcessor(mentions, authorField,
    "author_mention_matches_entity_value", (fv: FieldValue, m: Mention, begin: Int, end: Int) => {
      fv.valueId.isDefined &&
        PersonNameHelper.isNameMatch(m.words.slice(begin, end), fv.field.getValuePhrase(fv.valueId))
    }).run().asInstanceOf[AlignSegmentPredicate]
  authorMentionMatchesPredicate.featureValue = -1
  authorMentionMatchesPredicate.targetProportion = 0.9
  authorField.clearAll()
  constraintFns += authorMentionMatchesPredicate

  // if mention differs most of the time it is not aligned with entity
  val authorMentionDifferentPredicate = new NegationAlignSegmentPredicate("author_mention_differs_from_entity_value")
  authorMentionDifferentPredicate ++= authorMentionMatchesPredicate
  authorMentionDifferentPredicate.targetProportion = 0.1
  constraintFns += authorMentionDifferentPredicate

  // almost all title mentions are similar to entity values
  titleField.cacheAll()
  val titleMentionMatchesPredicate = new FieldMentionAlignPredicateProcessor(mentions, titleField,
    "title_mention_matches_entity_value", (fv: FieldValue, m: Mention, begin: Int, end: Int) => {
      fv.valueId.isDefined &&
        TitleHelper.isTitleSimilar(m.words.slice(begin, end), fv.field.getValuePhrase(fv.valueId))
    }).run().asInstanceOf[AlignSegmentPredicate]
  titleMentionMatchesPredicate.featureValue = -1
  titleMentionMatchesPredicate.targetProportion = 0.99
  titleField.clearAll()
  constraintFns += titleMentionMatchesPredicate

  // if mention differs almost all of the time it is not aligned with title entity
  val titleMentionDifferentPredicate = new NegationAlignSegmentPredicate("title_mention_differs_from_entity_value")
  titleMentionDifferentPredicate ++= titleMentionMatchesPredicate
  titleMentionDifferentPredicate.targetProportion = 0.01
  constraintFns += titleMentionDifferentPredicate

  // most booktitle mentions that are similar are aligned
  booktitleField.cacheAll()
  val booktitleMentionMatchesPredicate = new FieldMentionAlignPredicateProcessor(mentions, booktitleField,
    "booktitle_mention_matches_entity_value", (fv: FieldValue, m: Mention, begin: Int, end: Int) => {
      fv.valueId.isDefined &&
        BooktitleHelper.isBooktitleSimilar(m.words.slice(begin, end), fv.field.getValuePhrase(fv.valueId))
    }).run().asInstanceOf[AlignSegmentPredicate]
  booktitleMentionMatchesPredicate.featureValue = -1
  booktitleMentionMatchesPredicate.targetProportion = 0.7
  booktitleField.clearAll()
  constraintFns += booktitleMentionMatchesPredicate

  val booktitleMentionDiffersPredicate = new NegationAlignSegmentPredicate("booktitle_mention_differs_from_entity_value")
  booktitleMentionDiffersPredicate ++= booktitleMentionMatchesPredicate
  booktitleMentionDiffersPredicate.targetProportion = 0.3
  constraintFns += booktitleMentionDiffersPredicate

  // most journal mentions that are similar are aligned
  journalField.cacheAll()
  val journalMentionMatchesPredicate = new FieldMentionAlignPredicateProcessor(mentions, journalField,
    "journal_mention_matches_entity_value", (fv: FieldValue, m: Mention, begin: Int, end: Int) => {
      fv.valueId.isDefined &&
        JournalHelper.isJournalSimilar(m.words.slice(begin, end), fv.field.getValuePhrase(fv.valueId))
    }).run().asInstanceOf[AlignSegmentPredicate]
  journalMentionMatchesPredicate.featureValue = -1
  journalMentionMatchesPredicate.targetProportion = 0.7
  journalField.clearAll()
  constraintFns += journalMentionMatchesPredicate

  val journalMentionDiffersPredicate = new NegationAlignSegmentPredicate("journal_mention_differs_from_entity_value")
  journalMentionDiffersPredicate ++= journalMentionMatchesPredicate
  journalMentionDiffersPredicate.targetProportion = 0.3
  constraintFns += journalMentionDiffersPredicate

  // most citation mentions that are similar are aligned
  citationRecord.cacheAll()
  val citationMentionMatchesPredicate = new FieldMentionAlignPredicateProcessor(mentions, citationRecord,
    "citation_mention_matches_entity_value", (fv: FieldValue, m: Mention, begin: Int, end: Int) => {
      fv.valueId.isDefined &&
        CitationHelper.isCitationSimilar(m.words.slice(begin, end), fv.field.getValuePhrase(fv.valueId))
    }).run().asInstanceOf[AlignSegmentPredicate]
  citationMentionMatchesPredicate.featureValue = -1
  citationMentionMatchesPredicate.targetProportion = 0.7
  citationRecord.clearAll()
  constraintFns += citationMentionMatchesPredicate

  val citationMentionDiffersPredicate = new NegationAlignSegmentPredicate("citation_mention_differs_from_entity_value")
  citationMentionDiffersPredicate ++= citationMentionMatchesPredicate
  citationMentionDiffersPredicate.targetProportion = 0.3
  constraintFns += citationMentionDiffersPredicate

  val totalMentions = mentions.count.toInt
  // there is at most one title segment
  val countTitlePredicate = new CountFieldEmissionTypePredicate("title_count", titleField.name)
  val likelyTitleCount = (1.1 * titleMentionMatchesPredicate.map(_.mentionSegment.mentionId).size) / totalMentions
  logger.info("likely title count=" + likelyTitleCount)
  countTitlePredicate.targetProportion = 1
  constraintFns += countTitlePredicate

  // very few citation records have null title
  val noTitleEntityForCitationEntityPredicate = new RecordValuesFieldValueIsNullPredicate(
    "title_entity_null_for_citation_entity", titleField.name, citationRecord.name)
  noTitleEntityForCitationEntityPredicate.targetProportion = 0.1
  constraintFns += noTitleEntityForCitationEntityPredicate

  val noAuthorEntityForCitationEntityPredicate = new RecordValuesFieldValueIsNullPredicate(
    "author_entity_null_for_citation_entity", authorField.name, citationRecord.name)
  noAuthorEntityForCitationEntityPredicate.targetProportion = 0.1
  constraintFns += noAuthorEntityForCitationEntityPredicate

  val noBooktitleEntityForCitationEntityPredicate = new RecordValuesFieldValueIsNullPredicate(
    "booktitle_entity_null_for_citation_entity", booktitleField.name, citationRecord.name)
  noBooktitleEntityForCitationEntityPredicate.targetProportion = 0.5
  constraintFns += noBooktitleEntityForCitationEntityPredicate

  val noJournalEntityForCitationEntityPredicate = new RecordValuesFieldValueIsNullPredicate(
    "journal_entity_null_for_citation_entity", journalField.name, citationRecord.name)
  noJournalEntityForCitationEntityPredicate.targetProportion = 0.5
  constraintFns += noJournalEntityForCitationEntityPredicate

  val (params, constraintParams) = new SemiSupervisedJointSegmentationLearner(mentions, citationRecord)
    .learn(5, 20, 20, constraintFns, textWeight = 1e-1, evalQuery = env.evalQuery)
  logger.info("params: " + params)
  val evalName = env.rexaName
  val evalStats = new ConstrainedSegmentationEvaluator(evalName, mentions,
    params, constraintParams, constraintFns, citationRecord, true, env.evalQuery).run()
    .asInstanceOf[(Params, Option[PrintWriter], Option[PrintWriter])]._1
  TextSegmentationHelper.outputEval(evalName, evalStats, logger.info(_))
  new MentionWebpageStorer(mentions, evalName, citationRecord, params, constraintParams, constraintFns, 1500, 5000).run()
  EntityMemcachedClient.shutdown()
}

object Rexa1kMain extends RexaRunMain(Rexa1kEnv) with App

