package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._
import io.Source
import org.riedelcastro.nurupo.HasLogger
import java.io.PrintWriter
import collection.mutable.{ArrayBuffer, HashSet, HashMap}
import uk.ac.shef.wit.simmetrics.similaritymetrics.JaroWinkler

/**
 * @author kedar
 */

object BFTestRepo extends MongoRepository("bft_test")

object BFTestEnv {
  val repo = BFTestRepo
  val mentions = repo.mentionColl

  // attach features
  val MONTH = "(?:january|february|march|april|may|june|july|august|september|october|november|december|" +
    "jan|feb|apr|jun|jul|aug|sep|sept|oct|nov|dec)"
  val DOTW = "(?:mon|tues?|wed(?:nes)?|thurs?|fri|satu?r?|sun)(?:day)?"
  val PATHS = Source.fromFile("data/bft/bft_paths-c50-p1.txt").getLines()
    .map(_.split("\t")).map(tuple => tuple(1) -> tuple(0)).toMap
  val JWINK = new JaroWinkler

  def simplify(s: String): String = {
    if (s.matches("\\d+/\\d+(/\\d+)?")) "$date$"
    else if (s.matches("\\$\\d+(\\.\\d+)?")) "$price$"
    else if (s.matches(DOTW)) "$day$"
    else if (s.matches(MONTH)) "$month$"
    else s
  }

  def matchesRatingPattern(s: String): Boolean = s.matches("\\d+(\\.\\d+)?\\*") || s.matches("\\*\\d+(\\.\\d+)?")

  def matchesRatingPattern(phr: Seq[String]): Boolean = phr.length == 1 && matchesRatingPattern(phr(0))

  def fieldHashFn(phrase: Seq[String], transforms: Seq[(Seq[String], Seq[String])]): Seq[String] = {
    val hs = new HashSet[String]
    for (tphrase <- PhraseHash.transformedPhrases(phrase, transforms)) {
      hs ++= PhraseHash.ngramsWordHash(tphrase, Seq(1, 2))
    }
    hs.toSeq
  }

  def recordHashFn(full_phrase: Seq[String], transforms: Seq[(Seq[String], Seq[String])]): Seq[String] = {
    val phrase = full_phrase.filter(s => {
      !s.matches(".*\\d.*") && // ignore numerics
        !s.matches(DOTW) && // ignore day of the week
        !s.matches(MONTH) // ignore month
    })
    val hs = new HashSet[String]
    for (tphrase <- PhraseHash.transformedPhrases(phrase, transforms)) {
      hs ++= PhraseHash.ngramsWordHash(tphrase, Seq(1))
    }
    hs.toSeq
  }

  def isMentionPhraseContainedInValue(fv: FieldValue, m: Mention, begin: Int, end: Int,
                                      transforms: Seq[(Seq[String], Seq[String])]): Boolean = {
    if (!fv.valueId.isDefined) false
    else {
      def rmPunct(seq: Seq[String]) = seq.filter(!_.matches("^\\p{Punct}*$"))
      val mentionPhraseClean = rmPunct(m.words.slice(begin, end))
      val mentionTokenBeforePhrase: Seq[String] = if (begin == 0) Seq() else Seq(m.words(begin - 1))
      val mentionTokenAfterPhrase: Seq[String] = if (end == m.words.length) Seq() else Seq(m.words(end))
      val valuePhrase = fv.field.getValuePhrase(fv.valueId)
      def isContained(phrFrom: Seq[String], phrTo: Seq[String]): Boolean = {
        if (phrTo.length == 0) true
        else if (phrFrom.length < phrTo.length) false
        else {
          val phrFromSet = phrFrom.toSet
          for (wto <- phrTo) {
            if (!phrFromSet(wto) && !phrFromSet.exists(wfrom => JWINK.getSimilarity(wfrom, wto) >= 0.95))
              return false
          }
          true
        }
      }
      for (transformValuePhrase <- PhraseHash.transformedPhrases(valuePhrase, transforms)) {
        val transformValuePhraseClean = rmPunct(transformValuePhrase)
        if (isContained(transformValuePhraseClean, mentionPhraseClean) &&
          (mentionTokenBeforePhrase.length == 0 || !isContained(transformValuePhraseClean, mentionTokenBeforePhrase)) &&
          (mentionTokenAfterPhrase.length == 0 || !isContained(transformValuePhraseClean, mentionTokenAfterPhrase))) {
          // logger.info("before: " + mentionTokenBeforePhrase + " after: " + mentionTokenAfterPhrase + " phrase: " + m.words.slice(begin, end) + " value: " + valuePhrase)
          return true
        }
      }
      false
    }
  }

  var hotelnameTransforms = Seq(
    Seq("san", "diego") -> Seq("sd"),
    Seq("convention", "center") -> Seq("conv", "ctr")
  )

  var localareaTransforms = Seq(
    Seq("downtown") -> Seq("dt"),
    Seq("downtown") -> Seq("dtown"),
    Seq("downtown") -> Seq("dntwn"),
    Seq("airport", "pit") -> Seq("ap"),
    Seq("mission", "valley") -> Seq("mv")
  )

  var recordTransforms = hotelnameTransforms ++ localareaTransforms

  def updateRecordTransforms() {
    recordTransforms = hotelnameTransforms ++ localareaTransforms
  }

  def getMentionIds(query: DBObject = MongoDBObject()) =
    mentions.find(query, MongoDBObject()).map(_._id.get).toSeq
}

object BFTLoadRecordMentions extends MentionFileLoader(BFTestRepo.mentionColl, "data/bft/bft_records.txt", true)

object BFTLoadTextMentions extends MentionFileLoader(BFTestRepo.mentionColl, "data/bft/bft_texts.txt", false)

object BFTAttachPossibleEnds extends PossibleEndsAttacher(BFTestRepo.mentionColl, "possibleEndsAttacher") {
  def getPossibleEnds(words: Array[String]) = {
    val ends = Array.fill[Boolean](words.length + 1)(true)
    ends(0) = false
    ends
  }
}

object BFTAttachFeatures extends FeaturesAttacher(BFTestRepo.mentionColl, "featuresAttacher") {
  def getFeatures(words: Array[String]) = {
    import BFTestEnv._
    words.map(word => {
      Seq(simplify(word)) ++ {
        if (matchesRatingPattern(word)) Seq("contains_rating_pattern") else Seq.empty[String]
      } ++ {
        if (PATHS.contains(word)) Seq(PATHS(word)) else Seq.empty[String]
      }
    })
  }
}

object BFTMaxLengths extends MaxLengthsProcessor(BFTestRepo.mentionColl, true)

object BFTInitMain extends App {
  // clear repository
  BFTestRepo.clear()
  // load records
  BFTLoadRecordMentions.run()
  // load texts
  BFTLoadTextMentions.run()
  // attach ends
  BFTAttachPossibleEnds.run()
  // attach features
  BFTAttachFeatures.run()
}

class BFTBasicMain(val useOracle: Boolean) extends HasLogger {

  import BFTestEnv._

  // get maxlengths
  val maxLengths = BFTMaxLengths.run().asInstanceOf[HashMap[String, Int]]
  println("maxLengthMap=" + maxLengths)

  // create fields
  val otherField = SimpleField("O").setMaxSegmentLength(maxLengths("O")).init()
  val starratingField = SimpleField("starrating").setMaxSegmentLength(maxLengths("starrating")).init()
  val hotelnameField = SimpleField("hotelname").setMaxSegmentLength(maxLengths("hotelname")).init()
  val localareaField = SimpleField("localarea").setMaxSegmentLength(maxLengths("localarea")).init()

  // create record and add fields
  val listingRecord = SimpleRecord("listing").init()
  listingRecord.addField(otherField).addField(starratingField).addField(hotelnameField).addField(localareaField)

  // learn from records only
  val params = new SupervisedSegmentationOnlyLearner(mentions, listingRecord, useOracle).learn(50)
  logger.info("parameters: " + params)
  val evalStats = new DefaultSegmentationEvaluator("segmentation-only-oracle-" + useOracle, mentions,
    params, listingRecord).run().asInstanceOf[(Params, Option[PrintWriter], Option[PrintWriter])]._1
  TextSegmentationHelper.outputEval("segmentation-only-oracle-" + useOracle, evalStats, logger.info(_))
}

object BFTRecordSegmentationOnlyMain extends BFTBasicMain(false) with App

object BFTOracleSegmentationOnlyMain extends BFTBasicMain(true) with App

object BFTConstrainedSegmentationOnlyMain extends App with HasLogger {

  import BFTestEnv._

  // get maxlengths
  val maxLengths = BFTMaxLengths.run().asInstanceOf[HashMap[String, Int]]
  println("maxLengthMap=" + maxLengths)

  // create fields
  val otherField = SimpleField("O").setMaxSegmentLength(maxLengths("O")).init()
  val starratingField = SimpleField("starrating").setMaxSegmentLength(maxLengths("starrating")).init()
  val hotelnameField = SimpleField("hotelname").setMaxSegmentLength(maxLengths("hotelname")).init()
  val localareaField = SimpleField("localarea").setMaxSegmentLength(maxLengths("localarea")).init()

  // create record and add fields
  val listingRecord = SimpleRecord("listing").init()
  listingRecord.addField(otherField).addField(starratingField).addField(hotelnameField).addField(localareaField)

  // initialize constraints
  val constraintFns = new ArrayBuffer[ConstraintFunction]

  // >= 95% of pattern matches are ratings
  val ratingMatchesPatternPredicate = new FieldMentionAlignPredicateProcessor(mentions, starratingField,
    "rating_matches_pattern", (fv: FieldValue, m: Mention, begin: Int, end: Int) => {
      fv.field.name == starratingField.name && matchesRatingPattern(m.words.slice(begin, end))
    }).run().asInstanceOf[AlignSegmentPredicate]
  // featureValue=-1 since >= constraint
  ratingMatchesPatternPredicate.featureValue = -1
  ratingMatchesPatternPredicate.targetProportion = 0.95
  constraintFns += ratingMatchesPatternPredicate

  // <= 1% of not pattern matches are ratings
  val ratingNotMatchesPatternPredicate = new FieldMentionAlignPredicateProcessor(mentions, starratingField,
    "rating_not_matches_pattern", (fv: FieldValue, m: Mention, begin: Int, end: Int) => {
      fv.field.name == starratingField.name && !matchesRatingPattern(m.words.slice(begin, end))
    }).run().asInstanceOf[AlignSegmentPredicate]
  ratingNotMatchesPatternPredicate.targetProportion = 0.01
  constraintFns += ratingNotMatchesPatternPredicate

  // #hotelname <= 1 * numMentions
  val countHotelname = new CountFieldEmissionTypePredicate("count_hotelname", hotelnameField.name)
  countHotelname.targetProportion = 1
  constraintFns += countHotelname

  // #localarea <= 1 * numMentions
  val countLocalarea = new CountFieldEmissionTypePredicate("count_localarea", localareaField.name)
  countLocalarea.targetProportion = 1
  constraintFns += countLocalarea

  // #starrating <= 1 * numMentions
  val countRating = new CountFieldEmissionTypePredicate("count_rating", starratingField.name)
  countRating.targetProportion = 1
  constraintFns += countRating

  // limit count for each mention
  val addCountLimitPerMention = false
  if (addCountLimitPerMention) {
    for (mentionId <- getMentionIds()) {
      val instCountHotelname = new InstanceCountFieldEmissionType(mentionId, "count_hotelname", hotelnameField.name)
      instCountHotelname.targetProportion = 1
      constraintFns += instCountHotelname

      val instCountLocalarea = new InstanceCountFieldEmissionType(mentionId, "count_localarea", localareaField.name)
      instCountLocalarea.targetProportion = 1
      constraintFns += instCountLocalarea

      val instCountRating = new InstanceCountFieldEmissionType(mentionId, "count_rating", starratingField.name)
      instCountRating.targetProportion = 1
      constraintFns += instCountRating
    }
  }

  val (params, constraintParams) = new SemiSupervisedJointSegmentationLearner(mentions, listingRecord)
    .learn(constraintFns = constraintFns)
  val evalStats = new ConstrainedSegmentationEvaluator("semi-sup-segmentation", mentions,
    params, constraintParams, constraintFns, listingRecord, true).run()
    .asInstanceOf[(Params, Option[PrintWriter], Option[PrintWriter])]._1
  TextSegmentationHelper.outputEval("semi-sup-segmentation", evalStats, logger.info(_))
}

object BFTConstrainedAlignSegmentationSimpleMain extends App with HasLogger {

  import BFTestEnv._

  // get maxlengths
  val maxLengths = BFTMaxLengths.run().asInstanceOf[HashMap[String, Int]]
  println("maxLengthMap=" + maxLengths)

  // create fields
  val otherField = SimpleField("O").setMaxSegmentLength(maxLengths("O")).init()
  val starratingField = SimpleField("starrating").setMaxSegmentLength(maxLengths("starrating")).init()

  val hotelnameField = new SimpleEntityField("hotelname", repo, true)
    .setMaxSegmentLength(maxLengths("hotelname")).setHashCodes(fieldHashFn(_, hotelnameTransforms))
    .setSimilarities(0.3, 0.9).setMaxHashFraction(0.25).setPhraseUnique(true)
    .init().asInstanceOf[SimpleEntityField]

  val localareaField = new SimpleEntityField("localarea", repo, true)
    .setMaxSegmentLength(maxLengths("localarea")).setHashCodes(fieldHashFn(_, localareaTransforms))
    .setSimilarities(0.3, 0.9).setMaxHashFraction(0.25).setPhraseUnique(true)
    .init().asInstanceOf[SimpleEntityField]

  // create record and add fields
  val listingRecord = SimpleRecord("listing").init()
  listingRecord.addField(otherField).addField(starratingField).addField(hotelnameField).addField(localareaField)

  // initialize constraints
  val constraintFns = new ArrayBuffer[ConstraintFunction]

  // >= 95% of pattern matches are ratings
  val ratingMatchesPatternPredicate = new FieldMentionAlignPredicateProcessor(mentions, starratingField,
    "rating_matches_pattern", (fv: FieldValue, m: Mention, begin: Int, end: Int) => {
      fv.field.name == starratingField.name && matchesRatingPattern(m.words.slice(begin, end))
    }).run().asInstanceOf[AlignSegmentPredicate]
  // featureValue=-1 since >= constraint
  ratingMatchesPatternPredicate.featureValue = -1
  ratingMatchesPatternPredicate.targetProportion = 0.95
  constraintFns += ratingMatchesPatternPredicate

  // <= 1% of not pattern matches are ratings
  val ratingNotMatchesPatternPredicate = new FieldMentionAlignPredicateProcessor(mentions, starratingField,
    "rating_not_matches_pattern", (fv: FieldValue, m: Mention, begin: Int, end: Int) => {
      fv.field.name == starratingField.name && !matchesRatingPattern(m.words.slice(begin, end))
    }).run().asInstanceOf[AlignSegmentPredicate]
  ratingNotMatchesPatternPredicate.targetProportion = 0.01
  constraintFns += ratingNotMatchesPatternPredicate

  // >= 99% of hotelname segment maximal contained overlap are alignments
  val hotelnameMaximalContainedPredicate = new FieldMentionAlignPredicateProcessor(mentions, hotelnameField,
    "segment_maximal_contained_in_hotelname_value", (fv: FieldValue, m: Mention, begin: Int, end: Int) => {
      isMentionPhraseContainedInValue(fv, m, begin, end, hotelnameTransforms)
    }).run().asInstanceOf[AlignSegmentPredicate]
  hotelnameMaximalContainedPredicate.featureValue = -1
  hotelnameMaximalContainedPredicate.targetProportion = 0.99
  constraintFns += hotelnameMaximalContainedPredicate

  // <= 10% of hotelname segment not contained are alignments
  val hotelnameMaximalNotContainedPredicate = new FieldMentionAlignPredicateProcessor(mentions, hotelnameField,
    "segment_not_maximal_contained_in_hotelname_value", (fv: FieldValue, m: Mention, begin: Int, end: Int) => {
      fv.valueId.isDefined && !isMentionPhraseContainedInValue(fv, m, begin, end, hotelnameTransforms)
    }).run().asInstanceOf[AlignSegmentPredicate]
  hotelnameMaximalNotContainedPredicate.targetProportion = 0.01
  constraintFns += hotelnameMaximalNotContainedPredicate

  // >= 99% of localarea segment maximal contained overlap are alignments
  val localareaMaximalContainedPredicate = new FieldMentionAlignPredicateProcessor(mentions, localareaField,
    "segment_maximal_contained_in_localarea_value", (fv: FieldValue, m: Mention, begin: Int, end: Int) => {
      isMentionPhraseContainedInValue(fv, m, begin, end, localareaTransforms)
    }).run().asInstanceOf[AlignSegmentPredicate]
  localareaMaximalContainedPredicate.featureValue = -1
  localareaMaximalContainedPredicate.targetProportion = 0.99
  constraintFns += localareaMaximalContainedPredicate

  // <= 10% of localarea segment not contained are alignments
  val localareaMaximalNotContainedPredicate = new FieldMentionAlignPredicateProcessor(mentions, localareaField,
    "segment_not_maximal_contained_in_localarea_value", (fv: FieldValue, m: Mention, begin: Int, end: Int) => {
      fv.valueId.isDefined && !isMentionPhraseContainedInValue(fv, m, begin, end, localareaTransforms)
    }).run().asInstanceOf[AlignSegmentPredicate]
  localareaMaximalNotContainedPredicate.targetProportion = 0.01
  constraintFns += localareaMaximalNotContainedPredicate

  // #hotelname <= 1 * numMentions
  val countHotelname = new CountFieldEmissionTypePredicate("count_hotelname", hotelnameField.name)
  countHotelname.targetProportion = 1
  constraintFns += countHotelname

  // #localarea <= 1 * numMentions
  val countLocalarea = new CountFieldEmissionTypePredicate("count_localarea", localareaField.name)
  countLocalarea.targetProportion = 1
  constraintFns += countLocalarea

  // #starrating <= 1 * numMentions
  val countRating = new CountFieldEmissionTypePredicate("count_rating", starratingField.name)
  countRating.targetProportion = 1
  constraintFns += countRating

  val (params, constraintParams) = new SemiSupervisedJointSegmentationLearner(mentions, listingRecord)
    .learn(constraintFns = constraintFns, textWeight = 1e-1)
  val evalStats = new ConstrainedSegmentationEvaluator("semi-sup-segmentation", mentions,
    params, constraintParams, constraintFns, listingRecord, true).run()
    .asInstanceOf[(Params, Option[PrintWriter], Option[PrintWriter])]._1
  TextSegmentationHelper.outputEval("semi-sup-segmentation", evalStats, logger.info(_))
}

object BFTConstrainedAlignSegmentationClusterMain extends App with HasLogger {

  import BFTestEnv._

  // get maxlengths
  val maxLengths = BFTMaxLengths.run().asInstanceOf[HashMap[String, Int]]
  println("maxLengthMap=" + maxLengths)

  // create fields
  val otherField = SimpleField("O").setMaxSegmentLength(maxLengths("O")).init()
  val starratingField = SimpleField("starrating").setMaxSegmentLength(maxLengths("starrating")).init()

  val hotelnameField = new SimpleEntityField("hotelname", repo, true)
    .setMaxSegmentLength(maxLengths("hotelname")).setHashCodes(fieldHashFn(_, hotelnameTransforms))
    .setSimilarities(0.3, 0.9).setMaxHashFraction(0.25).setPhraseUnique(true)
    .init().asInstanceOf[SimpleEntityField]

  val localareaField = new SimpleEntityField("localarea", repo, true)
    .setMaxSegmentLength(maxLengths("localarea")).setHashCodes(fieldHashFn(_, localareaTransforms))
    .setSimilarities(0.3, 0.9).setMaxHashFraction(0.25).setPhraseUnique(true)
    .init().asInstanceOf[SimpleEntityField]

  // create record and add fields
  val listingRecord = new SimpleEntityRecord("listing", repo)
    .setHashCodes(recordHashFn(_, recordTransforms)).setSimilarities(0.4, 0.9)
    .setMaxHashFraction(0.25).setPhraseUnique(true)
    .init().asInstanceOf[SimpleEntityRecord]
  listingRecord.addField(otherField).addField(starratingField).addField(hotelnameField).addField(localareaField)

  // initialize constraints
  val constraintFns = new ArrayBuffer[ConstraintFunction]

  // >= 95% of pattern matches are ratings
  val ratingMatchesPatternPredicate = new FieldMentionAlignPredicateProcessor(mentions, starratingField,
    "rating_matches_pattern", (fv: FieldValue, m: Mention, begin: Int, end: Int) => {
      fv.field.name == starratingField.name && matchesRatingPattern(m.words.slice(begin, end))
    }).run().asInstanceOf[AlignSegmentPredicate]
  // featureValue=-1 since >= constraint
  ratingMatchesPatternPredicate.featureValue = -1
  ratingMatchesPatternPredicate.targetProportion = 0.95
  constraintFns += ratingMatchesPatternPredicate

  // <= 1% of not pattern matches are ratings
  val ratingNotMatchesPatternPredicate = new FieldMentionAlignPredicateProcessor(mentions, starratingField,
    "rating_not_matches_pattern", (fv: FieldValue, m: Mention, begin: Int, end: Int) => {
      fv.field.name == starratingField.name && !matchesRatingPattern(m.words.slice(begin, end))
    }).run().asInstanceOf[AlignSegmentPredicate]
  ratingNotMatchesPatternPredicate.targetProportion = 0.01
  constraintFns += ratingNotMatchesPatternPredicate

  // >= 99% of hotelname segment maximal contained overlap are alignments
  val hotelnameMaximalContainedPredicate = new FieldMentionAlignPredicateProcessor(mentions, hotelnameField,
    "segment_maximal_contained_in_hotelname_value", (fv: FieldValue, m: Mention, begin: Int, end: Int) => {
      isMentionPhraseContainedInValue(fv, m, begin, end, hotelnameTransforms)
    }).run().asInstanceOf[AlignSegmentPredicate]
  hotelnameMaximalContainedPredicate.featureValue = -1
  hotelnameMaximalContainedPredicate.targetProportion = 0.99
  constraintFns += hotelnameMaximalContainedPredicate

  // <= 10% of hotelname segment not contained are alignments
  val hotelnameMaximalNotContainedPredicate = new FieldMentionAlignPredicateProcessor(mentions, hotelnameField,
    "segment_not_maximal_contained_in_hotelname_value", (fv: FieldValue, m: Mention, begin: Int, end: Int) => {
      fv.valueId.isDefined && !isMentionPhraseContainedInValue(fv, m, begin, end, hotelnameTransforms)
    }).run().asInstanceOf[AlignSegmentPredicate]
  hotelnameMaximalNotContainedPredicate.targetProportion = 0.01
  constraintFns += hotelnameMaximalNotContainedPredicate

  // >= 99% of localarea segment maximal contained overlap are alignments
  val localareaMaximalContainedPredicate = new FieldMentionAlignPredicateProcessor(mentions, localareaField,
    "segment_maximal_contained_in_localarea_value", (fv: FieldValue, m: Mention, begin: Int, end: Int) => {
      isMentionPhraseContainedInValue(fv, m, begin, end, localareaTransforms)
    }).run().asInstanceOf[AlignSegmentPredicate]
  localareaMaximalContainedPredicate.featureValue = -1
  localareaMaximalContainedPredicate.targetProportion = 0.99
  constraintFns += localareaMaximalContainedPredicate

  // <= 10% of localarea segment not contained are alignments
  val localareaMaximalNotContainedPredicate = new FieldMentionAlignPredicateProcessor(mentions, localareaField,
    "segment_not_maximal_contained_in_localarea_value", (fv: FieldValue, m: Mention, begin: Int, end: Int) => {
      fv.valueId.isDefined && !isMentionPhraseContainedInValue(fv, m, begin, end, localareaTransforms)
    }).run().asInstanceOf[AlignSegmentPredicate]
  localareaMaximalNotContainedPredicate.targetProportion = 0.01
  constraintFns += localareaMaximalNotContainedPredicate

  // #hotelname <= 1 * numMentions
  val countHotelname = new CountFieldEmissionTypePredicate("count_hotelname", hotelnameField.name)
  countHotelname.targetProportion = 1
  constraintFns += countHotelname

  // #localarea <= 1 * numMentions
  val countLocalarea = new CountFieldEmissionTypePredicate("count_localarea", localareaField.name)
  countLocalarea.targetProportion = 1
  constraintFns += countLocalarea

  // #starrating <= 1 * numMentions
  val countRating = new CountFieldEmissionTypePredicate("count_rating", starratingField.name)
  countRating.targetProportion = 1
  constraintFns += countRating

  val (params, constraintParams) = new SemiSupervisedJointSegmentationLearner(mentions, listingRecord)
    .learn(constraintFns = constraintFns, textWeight = 1e-1)
  val evalStats = new ConstrainedSegmentationEvaluator("semi-sup-segmentation", mentions,
    params, constraintParams, constraintFns, listingRecord, true).run()
    .asInstanceOf[(Params, Option[PrintWriter], Option[PrintWriter])]._1
  TextSegmentationHelper.outputEval("semi-sup-segmentation", evalStats, logger.info(_))
}

object BFTMain extends App {

  import BFTestEnv._

  // get maxlengths
  val maxLengths = BFTMaxLengths.run().asInstanceOf[HashMap[String, Int]]
  println("maxLengthMap=" + maxLengths)

  // initialize default fields
  val otherField = SimpleField("O").setMaxSegmentLength(maxLengths("O")).init()
  val starratingField = SimpleField("starrating").setMaxSegmentLength(maxLengths("starrating")).init()

  def regexMatchesStarFn(mention: Mention, begin: Int, end: Int): Boolean = {
    matchesRatingPattern(mention.words.slice(begin, end))
  }

  def starratingAndRegexMatchesStarFn(fieldValue: FieldValue, mention: Mention, begin: Int, end: Int): Boolean = {
    fieldValue.field.name == "starrating" && regexMatchesStarFn(mention, begin, end)
  }

  def starratingAndNotRegexMatchesStarFn(fieldValue: FieldValue, mention: Mention, begin: Int, end: Int): Boolean = {
    fieldValue.field.name == "starrating" && !regexMatchesStarFn(mention, begin, end)
  }

  val starratingPredicate = new CountFieldEmissionTypePredicate("starrating_field", starratingField.name)
  val starratingAndRegexMatchesStarPredicate = new FieldMentionAlignPredicateProcessor(mentions, starratingField,
    "regex_matches_starrating", starratingAndRegexMatchesStarFn).run().asInstanceOf[AlignSegmentPredicate]
  println(starratingAndRegexMatchesStarPredicate.predicateName + "=" + starratingAndRegexMatchesStarPredicate.size +
    " #distinctIds=" + starratingAndRegexMatchesStarPredicate.map(_.mentionSegment.mentionId).size)
  val starratingAndNotRegexMatchesStarPredicate = new FieldMentionAlignPredicateProcessor(mentions, starratingField,
    "starrating_and_not_regex_matches_starrating", starratingAndNotRegexMatchesStarFn).run().asInstanceOf[AlignSegmentPredicate]
  println(starratingAndNotRegexMatchesStarPredicate.predicateName + "=" + starratingAndNotRegexMatchesStarPredicate.size +
    " #distinctIds=" + starratingAndNotRegexMatchesStarPredicate.map(_.mentionSegment.mentionId).size)

  // initialize entity fields
  val hotelnameField = new SimpleEntityField("hotelname", repo, true)
    .setMaxSegmentLength(maxLengths("hotelname")).setHashCodes(fieldHashFn(_, hotelnameTransforms))
    .setSimilarities(0.3, 0.9).setMaxHashFraction(0.25).setPhraseDuplicates(1)
    .init().asInstanceOf[SimpleEntityField]
  println(hotelnameField.getHashDocumentFrequency.filter(_._2 >= 20).mkString("\n"))

  val localareaField = new SimpleEntityField("localarea", repo, true)
    .setMaxSegmentLength(maxLengths("localarea")).setHashCodes(fieldHashFn(_, localareaTransforms))
    .setSimilarities(0.3, 0.9).setMaxHashFraction(0.25).setPhraseDuplicates(1)
    .init().asInstanceOf[SimpleEntityField]
  println(localareaField.getHashDocumentFrequency.filter(_._2 >= 10).mkString("\n"))

  // initialize entity record

  /*
  // useful for Gupta, Sarawagi joint extraction stuff
  val listingClustered = new ClusterEntityRecord("listing_cluster", repo)
    .setHashCodes(recordHashFn(_, recordTransforms)).setMinSim(0.9).setMaxHashFraction(0.25)
    .init().asInstanceOf[ClusterEntityRecord]

  var listingClusterIndex = 0
  for (cluster <- listingClustered.getRecordClusters) {
    listingClusterIndex += 1
    println()
    println("=== listing cluster[" + listingClusterIndex + "]")
    for (value <- cluster) {
      println("\t" + value)
    }
  }
  */

  val listingRecord = new SimpleEntityRecord("listing", repo)
    .setHashCodes(recordHashFn(_, recordTransforms)).setSimilarities(0.4, 0.9).setMaxHashFraction(0.25)
    .init().asInstanceOf[SimpleEntityRecord]
  listingRecord.addField(otherField).addField(starratingField).addField(hotelnameField).addField(localareaField)
  println("#fields=" + listingRecord.numFields)

  val constraintCountPredicates = Seq(starratingPredicate)
  val constraintExpectationPredicates = Seq(starratingAndRegexMatchesStarPredicate, starratingAndNotRegexMatchesStarPredicate)
  val firstDbo = mentions.findOne(MongoDBObject("isRecord" -> false, "cluster" -> "2")).get
  val firstMention = new Mention(firstDbo).setFeatures(firstDbo)
  val firstInferencer = new TestInferencer(firstMention, listingRecord,
    constraintCountFns = constraintCountPredicates, constraintExpectationFns = constraintExpectationPredicates)
  firstInferencer.updateCounts()
  println("counts: " + firstInferencer.counts)
  println("alignObsCounts: " + firstInferencer.constraintCounts)
  println("alignPredCounts: " + firstInferencer.constraintExpectations)

  if (true) System.exit(0)

  val uniqueClusters = new UniqueClusterProcessor(mentions).run().asInstanceOf[HashSet[String]]
  var numClusterExacts = 0
  for (cluster <- uniqueClusters) {
    var numTotal = 0
    val idToTotal = new HashMap[ObjectId, Int]
    for (dbo <- mentions.find(MongoDBObject("cluster" -> cluster))) {
      numTotal += 1
      val mention = new Mention(dbo)
      for (segment <- mention.fullSegments(listingRecord.name)) {
        val listingEntities = listingRecord.getPossibleValues(mention.id, segment.begin, segment.end)
        for (listingEntity <- listingEntities if listingEntity.valueId.isDefined) {
          idToTotal(listingEntity.valueId.get) = 1 + idToTotal.getOrElse(listingEntity.valueId.get, 0)
        }
      }
    }
    println("for cluster: " + cluster + " #total=" + numTotal + " #maxOverlap=" + idToTotal.values.max)
    if (numTotal == idToTotal.values.max) numClusterExacts += 1
  }
  println("foundClusters=" + numClusterExacts + "/" + uniqueClusters.size)

  // find close text mentions
  var numMatchFound = 0
  var numSomeMatchFound = 0
  var numTotal = 0
  var maxIds = 0
  for (dbo <- mentions.find()) {
    val mention = new Mention(dbo)
    numTotal += 1
    println()
    println("******************************************************")
    println("index: " + numTotal)
    println("isRecord: " + mention.isRecord)
    println("words: " + mention.words.mkString(" "))
    println("trueSeg: " + mention.trueWidget)
    println("trueCluster: " + mention.trueClusterOption)
    val entityValues = new HashSet[FieldValue]
    for (segment <- mention.fullSegments(listingRecord.name)) {
      val listingEntities = listingRecord.getPossibleValues(mention.id, segment.begin, segment.end)
      entityValues ++= listingEntities
    }
    var matchFound = false
    var someMatchFound = false
    maxIds = math.max(maxIds, entityValues.size)
    for (value <- entityValues) {
      for (entityMentionId <- value.field.getValueMention(value.valueId)) {
        val entityMention = new Mention(mentions.findOneByID(entityMentionId).get)
        val bothIdSame = entityMention.id == mention.id
        val bothClusterUndefined = !entityMention.trueClusterOption.isDefined && !mention.trueClusterOption.isDefined
        val bothClusterSame = entityMention.trueClusterOption == mention.trueClusterOption
        println("\t[predCluster: " + entityMention.trueClusterOption +
          ", isRecord: " + entityMention.isRecord + "]: " + entityMention.words.mkString(" "))
        if ((bothClusterUndefined && bothIdSame) || (bothClusterSame && entityMention.isRecord)) matchFound = true
        if (bothClusterUndefined || bothClusterSame) someMatchFound = true
      }
    }
    println("foundMatch=" + matchFound)
    println("foundMatchSome=" + someMatchFound)
    println("entityIdCount=" + entityValues.size)
    if (matchFound) numMatchFound += 1
    if (someMatchFound) numSomeMatchFound += 1
  }
  println("#found/total=" + numMatchFound + "/" + numTotal)
  println("#someFound/total=" + numSomeMatchFound + "/" + numTotal)
  println("#maxIds=" + maxIds)

  if (true) System.exit(0)

  // iterate over text mentions
  for (dbo <- mentions.find(MongoDBObject("isRecord" -> false))) {
    val mention = new Mention(dbo)
    println()
    println("******************************************************")
    println("words: " + mention.words.mkString(" "))
    println("trueSeg: " + mention.trueWidget)
    val entityValues = new HashSet[FieldValue]
    for (segment <- mention.possibleSegments(hotelnameField.name, hotelnameField.maxSegmentLength)) {
      val hotelEntities = hotelnameField.getPossibleValues(mention.id, segment.begin, segment.end)
      entityValues ++= hotelEntities
      // get mention ids from hotels
      /*
      val entityMentions = hotelEntities.map(fv => hotelnameField.getValueMention(fv.valueId))
      val localEntities = entityMentions.map(mid => localareaField.getMentionValues(mid)).flatMap(identity(_))
      entityValues ++= localEntities
      */
    }
    for (segment <- mention.possibleSegments(localareaField.name, localareaField.maxSegmentLength)) {
      val areaEntities = localareaField.getPossibleValues(mention.id, segment.begin, segment.end)
      entityValues ++= areaEntities
    }
    for (value <- entityValues) {
      val entField = value.field.asInstanceOf[SimpleEntityField]
      println("\t" + entField + ": " + entField.getValuePhrase(value.valueId) +
        " [mention=" + entField.getValueMention(value.valueId) + "]")
    }
  }
}