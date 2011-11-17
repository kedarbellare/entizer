package edu.umass.cs.iesl.entizer

import cc.refectorie.user.kedarb.dynprog.types.Hypergraph
import cc.refectorie.user.kedarb.dynprog.ProbStats
import org.riedelcastro.nurupo.HasLogger
import com.mongodb.casbah.Imports._
import collection.mutable.{HashMap, HashSet}
import optimization.projections._

/**
 * @author kedar
 */

case class SimpleInferSpec(viterbi: Boolean = false, trueSegmentInfer: Boolean = false,
                           bestUpdate: Boolean = false, stepSize: Double = 1,
                           constraintStepSize: Double = 0, constraintTargetStepSize: Double = 0)

trait ConstraintFunction {
  def groupKey(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
               mention: Mention, begin: Int, end: Int): Any = "default"

  def projection(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
                 mention: Mention, begin: Int, end: Int): Projection = PositivityProjection

  def defaultParamValue(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
                        mention: Mention, begin: Int, end: Int): Double = 0

  def apply(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
            mention: Mention, begin: Int, end: Int): Boolean

  def featureKey(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
                 mention: Mention, begin: Int, end: Int): Any

  def targetKey(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
                mention: Mention, begin: Int, end: Int): Any = mention.id

  def targetProportion: Double

  // if E_q[f_i] <= b_i then +1 else if E_q[f_i] >= b_i then -1
  def featureValue(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
                   mention: Mention, begin: Int, end: Int): Double
}

trait DefaultConstraintFunction extends ConstraintFunction {
  // by default uses <= style constraint
  var featureValue = 1.0
  // say 0.95 * numMentions * featureValue
  var targetProportion = 1.0

  def predicateName: String

  def featureValue(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
                   mention: Mention, begin: Int, end: Int) = featureValue

  def featureKey(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
                 mention: Mention, begin: Int, end: Int) = predicateName
}

class ExtractSegmentPredicate(val predicateName: String)
  extends HashSet[MentionSegment] with DefaultConstraintFunction {
  def apply(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
            mention: Mention, begin: Int, end: Int) = this(MentionSegment(mention.id, begin, end))
}

class AlignSegmentPredicate(val predicateName: String)
  extends HashSet[FieldValueMentionSegment] with DefaultConstraintFunction {
  def apply(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
            mention: Mention, begin: Int, end: Int) =
    this(FieldValueMentionSegment(currFieldValue, MentionSegment(mention.id, begin, end)))
}

class InstanceCountFieldEmissionType(val mentionId: ObjectId, val predicateName: String,
                                     val fieldType: String) extends DefaultConstraintFunction {
  override def groupKey(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
                        mention: Mention, begin: Int, end: Int) = "mention" -> mentionId

  def apply(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
            mention: Mention, begin: Int, end: Int) =
    mentionId == mention.id && currFieldValue.field.name == fieldType
}

class CountFieldEmissionTypePredicate(val predicateName: String, val fieldType: String)
  extends DefaultConstraintFunction {
  def apply(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
            mention: Mention, begin: Int, end: Int) =
    currFieldValue.field.name == fieldType
}

class CountFieldTransitionTypePredicate(val predicateName: String, val prevFieldType: String, val currFieldType: String)
  extends DefaultConstraintFunction {
  def apply(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
            mention: Mention, begin: Int, end: Int) =
    currFieldValue.field.name == currFieldType && prevFieldName == prevFieldType
}

trait SparseConstraintFunction extends ConstraintFunction {
  val targetProportion = 0.0

  lazy val projection = new SimplexProjection(sigma)

  def sigma: Double

  def noise: Double

  override def projection(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
                          mention: Mention, begin: Int, end: Int) = projection

  override def defaultParamValue(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
                                 mention: Mention, begin: Int, end: Int) = java.lang.Math.random() * noise

  def featureValue(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
                   mention: Mention, begin: Int, end: Int) = 1.0
}

class SparseFieldValuePerMention(val fieldType: String, val sigma: Double = 1.0, val noise: Double = 1e-4)
  extends SparseConstraintFunction {
  def apply(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
            mention: Mention, begin: Int, end: Int) =
    currFieldValue.valueId.isDefined && currFieldValue.field.name == fieldType

  override def groupKey(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
                        mention: Mention, begin: Int, end: Int) = ("sparse_per_mention", fieldType, mention.id)

  def featureKey(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
                 mention: Mention, begin: Int, end: Int) = currFieldValue.valueId
}

class SparseFieldValuesOverall(val fieldType: String, val sigma: Double = 1.0, val noise: Double = 1e-4)
  extends SparseConstraintFunction {
  def apply(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
            mention: Mention, begin: Int, end: Int) =
    currFieldValue.valueId.isDefined && currFieldValue.field.name == fieldType

  override def groupKey(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
                        mention: Mention, begin: Int, end: Int) = ("sparse_overall", fieldType, currFieldValue.valueId)

  def featureKey(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
                 mention: Mention, begin: Int, end: Int) = mention.id
}

class SparseRecordValuePerMention(val recordType: String, val sigma: Double = 1.0, val noise: Double = 1e-4,
                                  val startFieldName: String = "$START$")
  extends SparseConstraintFunction {
  def apply(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
            mention: Mention, begin: Int, end: Int) =
    rootFieldValue.valueId.isDefined && prevFieldName == startFieldName && rootFieldValue.field.name == recordType

  override def groupKey(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
                        mention: Mention, begin: Int, end: Int) = ("sparse_per_mention", recordType, mention.id)

  def featureKey(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
                 mention: Mention, begin: Int, end: Int) = rootFieldValue.valueId
}

class SparseRecordValuesOverall(val recordType: String, val sigma: Double = 1.0, val noise: Double = 1e-4,
                                val startFieldName: String = "$START$")
  extends SparseConstraintFunction {
  def apply(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
            mention: Mention, begin: Int, end: Int) =
    rootFieldValue.valueId.isDefined && prevFieldName == startFieldName && rootFieldValue.field.name == recordType

  override def groupKey(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
                        mention: Mention, begin: Int, end: Int) = ("sparse_overall", recordType, rootFieldValue.valueId)

  def featureKey(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
                 mention: Mention, begin: Int, end: Int) = mention.id
}

class SparseFieldValuePerRecordValue(val fieldType: String, val recordType: String,
                                     val sigma: Double = 1.0, val noise: Double = 1e-4)
  extends SparseConstraintFunction {
  def apply(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
            mention: Mention, begin: Int, end: Int) =
    rootFieldValue.valueId.isDefined && currFieldValue.valueId.isDefined &&
      rootFieldValue.field.name == recordType && currFieldValue.field.name == fieldType

  override def groupKey(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
                        mention: Mention, begin: Int, end: Int) = ("sparse", fieldType, recordType, rootFieldValue.valueId)

  def featureKey(rootFieldValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
                 mention: Mention, begin: Int, end: Int) = currFieldValue.valueId
}

trait ASimpleHypergraphInferencer[Widget] extends HasLogger {
  type Info = Hypergraph.HyperedgeInfo[Widget]

  val hypergraph = new Hypergraph[Widget]
  createHypergraph(hypergraph)
  hypergraph.computePosteriors(ispec.viterbi)
  val logZ = hypergraph.getLogZ
  val (bestWidget, logVZ) = {
    if (ispec.bestUpdate) {
      val result = hypergraph.fetchBestHyperpath(newWidget)
      (result.widget, result.logWeight)
    } else {
      (newWidget, Double.NaN)
    }
  }

  def updateCounts() {
    hypergraph.fetchPosteriors(ispec.viterbi)
  }

  // Main functions to override: specifies the entire model
  def createHypergraph(H: Hypergraph[Widget])

  def newWidget: Widget

  def stats = new ProbStats(logZ, logVZ, 0.0, 0.0, 0.0, 0.0)

  // inputs
  def example: Mention

  def params: Params

  def counts: Params

  def ispec: SimpleInferSpec
}

trait ASegmentationBasedInferencer extends ASimpleHypergraphInferencer[FieldValuesTextSegmentation] {
  type Widget = FieldValuesTextSegmentation

  lazy val N: Int = example.numTokens

  lazy val mentionId = example.id

  lazy val isRecord: Boolean = example.isRecord

  lazy val words: Seq[String] = example.words

  lazy val features: Seq[Seq[String]] = example.features

  lazy val trueSegmentation: TextSegmentation = example.trueWidget

  // (fieldName, inputPosition) => double
  lazy val cacheEmissionScores = new HashMap[(String, Int), Double]

  def newWidget = new Widget

  def startFieldName: String

  def root: FieldCollection

  def isAllowed(field: Field, begin: Int, end: Int): Boolean = {
    if (field.useFullSegment) {
      begin == 0 && end == N
    } else {
      if (ispec.trueSegmentInfer) {
        trueSegmentation.contains(TextSegment(field.name, begin, end))
      } else if (isRecord) {
        trueSegmentation.filter(seg => seg.begin == begin && seg.end == end).size > 0
      } else {
        example.possibleEnds(end)
      }
    }
  }

  def keyTransition(prevFieldName: String) = "transition" -> prevFieldName

  def scoreTransition(prevFieldName: String, currFieldName: String): Double = {
    if (isRecord) 0.0 else params.get(keyTransition(prevFieldName)).get(currFieldName)
  }

  def updateTransition(prevFieldName: String, currFieldName: String, count: Double) {
    if (!isRecord && ispec.stepSize != 0)
      counts.get(keyTransition(prevFieldName)).increment(currFieldName, ispec.stepSize * count)
  }

  def keyEmission(currFieldName: String) = "emission" -> currFieldName

  def scoreSingleEmission(currFieldName: String, position: Int): Double = {
    val emitKey = currFieldName -> position
    if (!cacheEmissionScores.contains(emitKey)) {
      cacheEmissionScores(emitKey) = params.get(keyEmission(currFieldName)).dot(features(position))
    }
    cacheEmissionScores(emitKey)
  }

  def updateSingleEmission(currFieldName: String, position: Int, count: Double) {
    if (ispec.stepSize != 0) counts.get(keyEmission(currFieldName)).increment(features(position), ispec.stepSize * count)
  }

  def scoreEmission(currFieldName: String, begin: Int, end: Int): Double = {
    var score = 0.0
    for (position <- begin until end) score += scoreSingleEmission(currFieldName, position)
    score
  }

  def updateEmission(currFieldName: String, begin: Int, end: Int, count: Double) {
    if (ispec.stepSize != 0)
      for (position <- begin until end) updateSingleEmission(currFieldName, position, count)
  }

  def keyAlignment(currFieldName: String) = "alignment" -> currFieldName

  def score(rootValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue, begin: Int, end: Int): Double

  def update(rootValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue, begin: Int, end: Int, prob: Double)

  def bestRecordClusterValue = bestWidget.head.values.head.valueId

  def bestFieldValueTextSegmentation: FieldValuesTextSegmentation = {
    val fldvalsegmentation = new FieldValuesTextSegmentation
    fldvalsegmentation ++= bestWidget.sortWith((v1: FieldValuesTextSegment, v2: FieldValuesTextSegment) => {
      v1.segment.begin < v2.segment.begin
    })
    fldvalsegmentation
  }

  def bestTextSegmentation: TextSegmentation = {
    val segmentation = new TextSegmentation
    segmentation ++= bestWidget.map(_.segment).sortWith((v1: TextSegment, v2: TextSegment) => {
      v1.begin < v2.begin
    })
    segmentation
  }

  def getRootValues(begin: Int, end: Int): HashSet[FieldValue] = {
    val rootValues = new HashSet[FieldValue]

    for (mentionRootValue <- root.getPossibleValues(mentionId, begin, end);
         segment <- root.getValueMentionSegment(mentionRootValue.valueId);
         rootValue <- root.getPossibleValues(segment.mentionId, segment.begin, segment.end)) {
      rootValues += rootValue
    }
    rootValues += FieldValue(root, None)
    rootValues
  }

  def getEntityRootPossibleValues(rootValue: FieldValue): HashSet[FieldValue] = {
    // logger.debug("root: " + rootValue)
    val rootMentionId = root.getValueMention(rootValue.valueId)
    val rootPossibleValues = new HashSet[FieldValue]

    // get possible values for the current root
    for (field <- root.getFields) {
      if (field.isInstanceOf[EntityField]) {
        for (mentionFieldValue <- field.getMentionValues(rootMentionId);
             segment <- field.getValueMentionSegment(mentionFieldValue.valueId);
             fieldValue <- field.getPossibleValues(segment.mentionId, segment.begin, segment.end)) {
          if (rootValue.valueId.isDefined && fieldValue.valueId.isDefined) {
            // logger.info("field: " + fieldValue + " for root: " + rootValue)
            rootPossibleValues += fieldValue
          }
        }
        // add null field for key value
        rootPossibleValues += FieldValue(field, None)
      } else {
        for (fieldValue <- field.getMentionValues(rootMentionId)) {
          // logger.debug("field: " + fieldValue)
          rootPossibleValues += fieldValue
        }
      }
    }
    // logger.debug("possible values: " + rootPossibleValues)
    rootPossibleValues
  }

  def getRootPossibleValues(rootValue: FieldValue): HashSet[FieldValue] = {
    if (root.isInstanceOf[SimpleEntityRecord]) getEntityRootPossibleValues(rootValue)
    else null.asInstanceOf[HashSet[FieldValue]]
  }

  def createHypergraph(H: Hypergraph[Widget]) {
    def gen(rootValue: FieldValue, rootPossibleValues: HashSet[FieldValue],
            prevFieldValue: FieldValue, begin: Int): Object = {
      if (begin == N) H.endNode
      else {
        val node = (Seq(rootValue, prevFieldValue), begin)
        // logger.debug("node: " + node)
        if (H.addSumNode(node)) {
          var tmpadded = 0
          for (currField <- root.getFields) {
            for (end <- (begin + 1) to math.min(begin + currField.maxSegmentLength, N)
                 if isAllowed(currField, begin, end)) {
              for (currFieldValue <- currField.getPossibleValues(mentionId, begin, end)
                   if rootPossibleValues == null || rootPossibleValues(currFieldValue)) {
                tmpadded += 1
                // logger.info("Using " + Seq(rootValue, currFieldValue) + " for generating '" + words.slice(begin, end).mkString(" ") + "'")
                H.addEdge(node, gen(rootValue, rootPossibleValues, currFieldValue, end), new Info {
                  def getWeight = score(rootValue, prevFieldValue.field.name, currFieldValue, begin, end)

                  def setPosterior(count: Double) {
                    update(rootValue, prevFieldValue.field.name, currFieldValue, begin, end, count)
                  }

                  def choose(widget: Widget) = {
                    widget += FieldValuesTextSegment(Seq(rootValue, currFieldValue), TextSegment(currField.name, begin, end))
                    widget
                  }
                })
              }
            }
          }
          if (tmpadded == 0) logger.error("no values found for " + words + " @ " + begin + "\npossible=" +
            rootPossibleValues + "\nroot=" + getRootValues(0, N))
        }
        node
      }
    }

    // logger.debug("mention: " + example)
    // generate
    val begin = 0
    for (end <- (begin + 1) to math.min(begin + root.maxSegmentLength, N) if isAllowed(root, begin, end)) {
      for (rootValue <- getRootValues(begin, end)) {
        val rootPossibleValues = getRootPossibleValues(rootValue)
        // generate using rootValue and root possible values
        for (currField <- root.getFields) {
          for (end <- (begin + 1) to math.min(begin + currField.maxSegmentLength, N)
               if isAllowed(currField, begin, end)) {
            for (currFieldValue <- currField.getPossibleValues(mentionId, begin, end)
                 if rootPossibleValues == null || rootPossibleValues(currFieldValue)) {
              // logger.info("Using " + Seq(rootValue, currFieldValue) + " for generating '" + words.slice(begin, end).mkString(" ") + "'")
              H.addEdge(H.sumStartNode(), gen(rootValue, rootPossibleValues, currFieldValue, end), new Info {
                def getWeight = score(rootValue, startFieldName, currFieldValue, begin, end)

                def setPosterior(count: Double) {
                  update(rootValue, startFieldName, currFieldValue, begin, end, count)
                }

                def choose(widget: Widget) = {
                  widget += FieldValuesTextSegment(Seq(rootValue, currFieldValue), TextSegment(currField.name, begin, end))
                  widget
                }
              })
            }
          }
        }
      }
    }
  }
}

class DefaultSegmentationInferencer(val root: FieldCollection, val example: Mention,
                                    val params: Params, val counts: Params, val ispec: SimpleInferSpec,
                                    val startFieldName: String = "$START$")
  extends ASegmentationBasedInferencer {
  override def getRootValues(begin: Int, end: Int) = super.getRootValues(begin, end).filter(!_.valueId.isDefined)

  def score(rootValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
            begin: Int, end: Int) = {
    val currField = currFieldValue.field
    val currFieldName = currField.name
    scoreTransition(prevFieldName, currFieldName) + scoreEmission(currFieldName, begin, end)
  }

  def update(rootValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue,
             begin: Int, end: Int, prob: Double) {
    val currField = currFieldValue.field
    val currFieldName = currField.name
    updateTransition(prevFieldName, currFieldName, prob)
    updateEmission(currFieldName, begin, end, prob)
  }
}

class ConstrainedSegmentationInferencer(val root: FieldCollection, val example: Mention,
                                        val params: Params, val counts: Params,
                                        val constraintParams: Params, val constraintCounts: Params,
                                        val constraintFns: Seq[ConstraintFunction], val ispec: SimpleInferSpec,
                                        val constraintTargets: Params = null, val startFieldName: String = "$START$")
  extends ASegmentationBasedInferencer {
  // group -> feat
  lazy val updatedConstraintTargets = new HashSet[(Any, Any, Any)]

  def score(rootValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue, begin: Int, end: Int) = {
    var score = 0.0
    val currField = currFieldValue.field
    val currFieldName = currField.name
    score += scoreTransition(prevFieldName, currFieldName)
    score += scoreEmission(currFieldName, begin, end)
    for (constraintFn <- constraintFns) {
      if (constraintFn(rootValue, prevFieldName, currFieldValue, example, begin, end)) {
        val group = constraintFn.groupKey(rootValue, prevFieldName, currFieldValue, example, begin, end)
        if (!constraintParams.contains(group)) {
          val projection = constraintFn.projection(rootValue, prevFieldName, currFieldValue, example, begin, end)
          constraintParams.get(group, projection)
        }
        val feat = constraintFn.featureKey(rootValue, prevFieldName, currFieldValue, example, begin, end)
        if (!constraintParams.get(group).contains(feat)) {
          val value = constraintFn.defaultParamValue(rootValue, prevFieldName, currFieldValue, example, begin, end)
          constraintParams.get(group).set(feat, value)
        }
        score -= constraintParams.get(group).get(feat) * constraintFn.featureValue(rootValue, prevFieldName,
          currFieldValue, example, begin, end)
      }
    }
    score
  }

  def update(rootValue: FieldValue, prevFieldName: String, currFieldValue: FieldValue, begin: Int, end: Int,
             prob: Double) {
    val currField = currFieldValue.field
    val currFieldName = currField.name
    updateTransition(prevFieldName, currFieldName, prob)
    updateEmission(currFieldName, begin, end, prob)
    if (ispec.constraintStepSize != 0 || ispec.constraintTargetStepSize != 0) {
      for (constraintFn <- constraintFns) {
        if (constraintFn(rootValue, prevFieldName, currFieldValue, example, begin, end)) {
          // update constraint counts
          val group = constraintFn.groupKey(rootValue, prevFieldName, currFieldValue, example, begin, end)
          if (!constraintCounts.contains(group)) {
            val projection = constraintFn.projection(rootValue, prevFieldName, currFieldValue, example, begin, end)
            constraintCounts.get(group, projection)
          }
          val feat = constraintFn.featureKey(rootValue, prevFieldName, currFieldValue, example, begin, end)
          val featval = constraintFn.featureValue(rootValue, prevFieldName, currFieldValue, example, begin, end)
          constraintCounts.get(group).increment(feat, -ispec.constraintStepSize * featval * prob)
          // update constraint targets if present
          if (constraintTargets != null) {
            val targ = constraintFn.targetKey(rootValue, prevFieldName, currFieldValue, example, begin, end)
            val uniqTargKey = (group, feat, targ)
            if (!updatedConstraintTargets(uniqTargKey)) {
              constraintTargets.get(group).increment(feat,
                -ispec.constraintTargetStepSize * featval * constraintFn.targetProportion)
              updatedConstraintTargets += uniqTargKey
            }
          }
        }
      }
    }
  }
}