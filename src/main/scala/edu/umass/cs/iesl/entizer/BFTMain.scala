package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._
import io.Source
import org.riedelcastro.nurupo.HasLogger
import java.io.PrintWriter
import collection.mutable.{ArrayBuffer, HashSet, HashMap}

/**
 * @author kedar
 */

object BFTestRepo extends MongoRepository("bft_test")

object BFTestEnv extends Env {
  val repo = BFTestRepo
  val mentions = repo.mentionColl
  val evalQuery = MongoDBObject("isRecord" -> false, "cluster" -> MongoDBObject("$exists" -> true))

  // attach features
  val MONTH = "(?:january|february|march|april|may|june|july|august|september|october|november|december|" +
    "jan|feb|apr|jun|jul|aug|sep|sept|oct|nov|dec)"
  val DOTW = "(?:mon|tues?|wed(?:nes)?|thurs?|fri|satu?r?|sun)(?:day)?"
  val PATHS = Source.fromFile("data/bft/bft_paths-c50-p1.txt").getLines()
    .map(_.split("\t")).map(tuple => tuple(1) -> tuple(0)).toMap

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

  var hotelnameTransforms = Seq(
    Seq("hawthorn") -> Seq("hawthorne"),
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
  val evalName = "bft-segmentation-only-uses-texts-" + useOracle
  val evalStats = new DefaultSegmentationEvaluator(evalName, mentions, params, listingRecord, true, evalQuery).run()
    .asInstanceOf[(Params, Option[PrintWriter], Option[PrintWriter])]._1
  TextSegmentationHelper.outputEval(evalName, evalStats, logger.info(_))
  new MentionWebpageStorer(mentions, evalName, listingRecord, params, null, null).run()
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

  // >= 99% of pattern matches are ratings
  val ratingMatchesPatternPredicate = new FieldMentionAlignPredicateProcessor(mentions, starratingField,
    "rating_matches_pattern", (fv: FieldValue, m: Mention, begin: Int, end: Int) => {
      fv.field.name == starratingField.name && matchesRatingPattern(m.words.slice(begin, end))
    }).run().asInstanceOf[AlignSegmentPredicate]
  // featureValue=-1 since >= constraint
  ratingMatchesPatternPredicate.featureValue = -1
  ratingMatchesPatternPredicate.targetProportion = 0.99
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
  val evalName = "bft-semisup-segmentation-only"
  val evalStats = new ConstrainedSegmentationEvaluator(evalName, mentions,
    params, constraintParams, constraintFns, listingRecord, true, evalQuery).run()
    .asInstanceOf[(Params, Option[PrintWriter], Option[PrintWriter])]._1
  TextSegmentationHelper.outputEval(evalName, evalStats, logger.info(_))
  new MentionWebpageStorer(mentions, evalName, listingRecord, params, constraintParams, constraintFns).run()
}

class BFTConstrainedAlignSegmentation(val numHotelDups: Int, val numAreaDups: Int, val numListingDups: Int,
                                      val doRecordClustering: Boolean, val useSparsity: Boolean) extends HasLogger {

  import BFTestEnv._

  // get maxlengths
  val maxLengths = BFTMaxLengths.run().asInstanceOf[HashMap[String, Int]]
  println("maxLengthMap=" + maxLengths)

  // create fields
  val otherField = SimpleField("O").setMaxSegmentLength(maxLengths("O")).init()
  val starratingField = SimpleField("starrating").setMaxSegmentLength(maxLengths("starrating")).init()

  val hotelnameField = new SimpleEntityField("hotelname", repo, true)
    .setMaxSegmentLength(maxLengths("hotelname")).setHashCodes(fieldHashFn(_, hotelnameTransforms))
    .setSimilarities(0.3, 0.9).setMaxHashFraction(0.25).setPhraseDuplicates(numHotelDups)
    .init().asInstanceOf[SimpleEntityField]

  val localareaField = new SimpleEntityField("localarea", repo, true)
    .setMaxSegmentLength(maxLengths("localarea")).setHashCodes(fieldHashFn(_, localareaTransforms))
    .setSimilarities(0.3, 0.9).setMaxHashFraction(0.25).setPhraseDuplicates(numAreaDups)
    .init().asInstanceOf[SimpleEntityField]

  // create record and add fields
  val listingRecord: FieldCollection = {
    if (doRecordClustering) new SimpleEntityRecord("listing", repo)
      .setHashCodes(recordHashFn(_, recordTransforms)).setSimilarities(0.4, 0.9)
      .setMaxHashFraction(0.25).setPhraseDuplicates(numListingDups)
      .init().asInstanceOf[SimpleEntityRecord]
    else SimpleRecord("listing").init()
  }
  listingRecord.addField(otherField).addField(starratingField).addField(hotelnameField).addField(localareaField)

  // initialize constraints
  val constraintFns = new ArrayBuffer[ConstraintFunction]

  // >= 99% of pattern matches are ratings
  val ratingMatchesPatternPredicate = new FieldMentionAlignPredicateProcessor(mentions, starratingField,
    "rating_matches_pattern", (fv: FieldValue, m: Mention, begin: Int, end: Int) => {
      fv.field.name == starratingField.name && matchesRatingPattern(m.words.slice(begin, end))
    }).run().asInstanceOf[AlignSegmentPredicate]
  // featureValue=-1 since >= constraint
  ratingMatchesPatternPredicate.featureValue = -1
  ratingMatchesPatternPredicate.targetProportion = 0.99
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
      isMentionPhraseApproxContainedInValue(fv, m, begin, end, hotelnameTransforms)
    }).run().asInstanceOf[AlignSegmentPredicate]
  removeSubsegmentAligns(hotelnameMaximalContainedPredicate)
  hotelnameMaximalContainedPredicate.featureValue = -1
  hotelnameMaximalContainedPredicate.targetProportion = 0.9
  constraintFns += hotelnameMaximalContainedPredicate

  // <= 10% of hotelname segment not contained are alignments
  val hotelnameMaximalNotContainedPredicate = new FieldMentionAlignPredicateProcessor(mentions, hotelnameField,
    "segment_not_maximal_contained_in_hotelname_value", (fv: FieldValue, m: Mention, begin: Int, end: Int) => {
      fv.valueId.isDefined && !isMentionPhraseApproxContainedInValue(fv, m, begin, end, hotelnameTransforms)
    }).run().asInstanceOf[AlignSegmentPredicate]
  hotelnameMaximalNotContainedPredicate.targetProportion = 0.1
  constraintFns += hotelnameMaximalNotContainedPredicate

  // >= 99% of localarea segment maximal contained overlap are alignments
  val localareaMaximalContainedPredicate = new FieldMentionAlignPredicateProcessor(mentions, localareaField,
    "segment_maximal_contained_in_localarea_value", (fv: FieldValue, m: Mention, begin: Int, end: Int) => {
      isMentionPhraseApproxContainedInValue(fv, m, begin, end, localareaTransforms)
    }).run().asInstanceOf[AlignSegmentPredicate]
  removeSubsegmentAligns(localareaMaximalContainedPredicate)
  localareaMaximalContainedPredicate.featureValue = -1
  localareaMaximalContainedPredicate.targetProportion = 0.9
  constraintFns += localareaMaximalContainedPredicate

  // <= 10% of localarea segment not contained are alignments
  val localareaMaximalNotContainedPredicate = new FieldMentionAlignPredicateProcessor(mentions, localareaField,
    "segment_not_maximal_contained_in_localarea_value", (fv: FieldValue, m: Mention, begin: Int, end: Int) => {
      fv.valueId.isDefined && !isMentionPhraseApproxContainedInValue(fv, m, begin, end, localareaTransforms)
    }).run().asInstanceOf[AlignSegmentPredicate]
  localareaMaximalNotContainedPredicate.targetProportion = 0.1
  constraintFns += localareaMaximalNotContainedPredicate

  val totalMentions = getMentionIds().size
  // #hotelname <= 1 * numMentions
  val countHotelname = new CountFieldEmissionTypePredicate("count_hotelname", hotelnameField.name)
  val likelyHotelCount = (1.0 * hotelnameMaximalContainedPredicate.map(_.mentionSegment.mentionId).size) / totalMentions
  logger.info("Hotel count=" + likelyHotelCount)
  countHotelname.targetProportion = 1 // likelyHotelCount
  constraintFns += countHotelname

  // #localarea <= 1 * numMentions
  val countLocalarea = new CountFieldEmissionTypePredicate("count_localarea", localareaField.name)
  val likelyAreaCount = (1.0 * localareaMaximalContainedPredicate.map(_.mentionSegment.mentionId).size) / totalMentions
  logger.info("Localarea count=" + likelyAreaCount)
  countLocalarea.targetProportion = 1 // likelyAreaCount
  constraintFns += countLocalarea

  // #starrating <= 1 * numMentions
  val countRating = new CountFieldEmissionTypePredicate("count_rating", starratingField.name)
  val likelyRatingCount = (1.0 * ratingMatchesPatternPredicate.map(_.mentionSegment.mentionId).size) / totalMentions
  logger.info("Rating count=" + likelyRatingCount)
  countRating.targetProportion = 1 // likelyRatingCount
  constraintFns += countRating

  // add sparsity for hotelname
  if (useSparsity) {
    // each mention should have few hotelnames
    constraintFns += new SparseFieldValuePerMention(hotelnameField.name, 5)
    // there should be relatively few hotels
    constraintFns += new SparseFieldValuesOverall(hotelnameField.name, 0.1)
    // each mention should have few localareas
    constraintFns += new SparseFieldValuePerMention(localareaField.name, 5)
    // there should be relatively few areas
    constraintFns += new SparseFieldValuesOverall(localareaField.name, 0.1)
    if (doRecordClustering) {
      // each mention should have very few listings
      constraintFns += new SparseRecordValuePerMention(listingRecord.name, 5)
      // there should be relatively few listings
      constraintFns += new SparseRecordValuesOverall(listingRecord.name, 0.1)
      // each listing entity should have very few hotel entities
      constraintFns += new SparseFieldValuePerRecordValue(hotelnameField.name, listingRecord.name, 0.1)
      constraintFns += new SparseFieldValuePerRecordValue(localareaField.name, listingRecord.name, 0.1)
    }
  }

  val (params, constraintParams) = new SemiSupervisedJointSegmentationLearner(mentions, listingRecord)
    .learn(constraintFns = constraintFns, textWeight = 1e-1)
  val evalName = "bft-hotels-" + numHotelDups + "-areas-" + numAreaDups + "-listings-" + numListingDups +
    "-recordcluster-" + doRecordClustering + "-sparse-" + useSparsity
  val evalStats = new ConstrainedSegmentationEvaluator(evalName, mentions,
    params, constraintParams, constraintFns, listingRecord, true, evalQuery).run()
    .asInstanceOf[(Params, Option[PrintWriter], Option[PrintWriter])]._1
  TextSegmentationHelper.outputEval(evalName, evalStats, logger.info(_))
  new MentionWebpageStorer(mentions, evalName, listingRecord, params, constraintParams, constraintFns).run()
}

object BFTConstrainedAlignSegmentationSimpleMain extends BFTConstrainedAlignSegmentation(1, 1, 1, false, false) with App

object BFTConstrainedAlignSegmentationFieldClusterMain extends BFTConstrainedAlignSegmentation(5, 5, 1, false, true) with App

object BFTConstrainedAlignSegmentationClusterMain extends BFTConstrainedAlignSegmentation(5, 5, 5, true, false) with App

object BFTConstrainedAlignSegmentationClusterSparseMain extends BFTConstrainedAlignSegmentation(5, 5, 5, true, true) with App

object BFTMain extends App