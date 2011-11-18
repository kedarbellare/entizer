package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._
import org.riedelcastro.nurupo.HasLogger
import cc.refectorie.user.kedarb.dynprog.ProbStats
import optimization.stopCriteria.AverageValueDifference
import optimization.gradientBasedMethods.stats.OptimizerStats
import java.io.PrintWriter
import optimization.linesearch.{InterpolationPickFirstStep, ArmijoLineSearchMinimizationAlongProjectionArc, ArmijoLineSearchMinimization}
import optimization.gradientBasedMethods._

/**
 * @author kedar
 */

abstract class ParameterProcessor(root: FieldCollection, initParams: Params, useOracle: Boolean)
  extends ParallelCollectionProcessor {
  def inputJob = JobCenter.Job(
    query = (if (useOracle) MongoDBObject() else MongoDBObject("isRecord" -> true)),
    select = MongoDBObject("isRecord" -> 1, "words" -> 1, "features" -> 1, "bioLabels" -> 1,
      "possibleEnds" -> 1, "cluster" -> 1))

  override def newOutputParams(isMaster: Boolean = false) = {
    val params = new Params
    params.copy(initParams)
    (params, new ProbStats())
  }

  override def merge(outputParams: Any, partialOutputParams: Any) {
    val output = outputParams.asInstanceOf[(Params, ProbStats)]
    val partialOutput = partialOutputParams.asInstanceOf[(Params, ProbStats)]
    output._1.increment(partialOutput._1, 1)
    output._2 += partialOutput._2
  }

  def processMention(mention: Mention, partialParams: Params, partialStats: ProbStats)

  def process(dbo: DBObject, inputParams: Any, partialOutputParams: Any) {
    val mention = new Mention(dbo).setFeatures(dbo)
    val partialOutput = partialOutputParams.asInstanceOf[(Params, ProbStats)]
    processMention(mention, partialOutput._1, partialOutput._2)
  }
}

class DefaultParameterInitializer(val root: FieldCollection, val inputColl: MongoCollection, val initParams: Params,
                                  val useOracle: Boolean = true)
  extends ParameterProcessor(root, initParams, useOracle) {
  def name = "defaultParameterInitializer"

  def processMention(mention: Mention, partialParams: Params, partialStats: ProbStats) {
    new DefaultSegmentationInferencer(root, mention, partialParams, partialParams, SimpleInferSpec())
  }
}

class ConstrainedParameterInitializer(val root: FieldCollection, val inputColl: MongoCollection, val params: Params,
                                      val initConstraintParams: Params, val constraintFns: Seq[ConstraintFunction],
                                      val useOracle: Boolean = true)
  extends ParameterProcessor(root, initConstraintParams, useOracle) {
  def name = "constrainedParameterInitializer"

  def processMention(mention: Mention, partialParams: Params, partialStats: ProbStats) {
    new ConstrainedSegmentationInferencer(root, mention, params, params, partialParams, partialParams, constraintFns, SimpleInferSpec())
  }
}

// Parameters should be initialized before passing it to objective
abstract class ACRFObjective(params: Params, invVariance: Double) extends ProjectedObjective with HasLogger {
  var objectiveValue = Double.NaN

  // set parameters and gradient
  val numParams = params.numParams
  parameters = new Array[Double](numParams)
  params.getParams(parameters)
  gradient = new Array[Double](numParams)

  override def getParameter(index: Int) = parameters(index)

  override def setParameter(index: Int, value: Double) {
    updateCalls += 1
    objectiveValue = Double.NaN
    parameters(index) = value
  }

  override def getParameters = parameters

  override def setParameters(params: Array[Double]) {
    updateCalls += 1
    objectiveValue = Double.NaN
    Array.copy(params, 0, parameters, 0, params.length)
  }

  override def setInitialParameters(params: Array[Double]) {
    setParameters(params)
  }

  def getValueAndGradient: (Params, ProbStats)

  def updateValueAndGradient() {
    // set parameters as they may have changed
    params.setParams(parameters)
    val (expectations, stats) = getValueAndGradient
    // output objective
    val paramsTwoNormSquared = parameters.par.map(x => x * x).reduce(_ + _)
    objectiveValue = 0.5 * paramsTwoNormSquared * invVariance - stats.logZ
    logger.info("objective=" + objectiveValue)
    // compute gradient
    java.util.Arrays.fill(gradient, 0.0)
    expectations.increment(params, -invVariance)
    // point in correct direction
    expectations.mult(-1)
    // move expectations to gradient
    expectations.getParams(gradient)
  }

  def getValue = {
    if (objectiveValue.isNaN) {
      functionCalls += 1
      updateValueAndGradient()
    }
    objectiveValue
  }

  def getGradient = {
    if (objectiveValue.isNaN) {
      gradientCalls += 1
      updateValueAndGradient()
    }
    gradient
  }

  def projectPoint(point: Array[Double]): Array[Double] = params.projectParams(point)

  override def toString = "objective = " + objectiveValue
}

trait SegmentationEvaluator extends ParallelCollectionProcessor {
  def evalName: String

  def outputWhatsWrong: Boolean

  def evalQuery: MongoDBObject

  def inputJob = JobCenter.Job(
    query = evalQuery,
    select = MongoDBObject("isRecord" -> 1, "words" -> 1, "features" -> 1, "bioLabels" -> 1, "possibleEnds" -> 1,
      "cluster" -> 1))

  override def newOutputParams(isMaster: Boolean = false) = {
    val oid = new ObjectId
    val oidStr = oid.toString
    val trueWriterOpt: Option[PrintWriter] =
      if (isMaster) None
      else if (outputWhatsWrong) {
        new java.io.File(evalName).mkdirs()
        Some(new PrintWriter("%s/%s.true.txt".format(evalName, oidStr)))
      }
      else None
    val predWriterOpt: Option[PrintWriter] =
      if (isMaster) None
      else if (outputWhatsWrong) {
        new java.io.File(evalName).mkdirs()
        Some(new PrintWriter("%s/%s.pred.txt".format(evalName, oidStr)))
      }
      else None
    (new Params, trueWriterOpt, predWriterOpt)
  }

  override def merge(outputParams: Any, partialOutputParams: Any) {
    val output = outputParams.asInstanceOf[(Params, Option[PrintWriter], Option[PrintWriter])]
    val partialOutput = partialOutputParams.asInstanceOf[(Params, Option[PrintWriter], Option[PrintWriter])]
    output._1.increment(partialOutput._1, 1)
    for (writer <- partialOutput._2) writer.close()
    for (writer <- partialOutput._3) writer.close()
  }

  def getTrueWidget(mention: Mention): TextSegmentation =
    TextSegmentationHelper.adjustSegmentation(mention.words, mention.trueWidget)

  def getPredWidget(mention: Mention): TextSegmentation

  def process(dbo: DBObject, inputParams: Any, partialOutputParams: Any) {
    import TextSegmentationHelper._
    val mention = new Mention(dbo).setFeatures(dbo)
    val partialOutput = partialOutputParams.asInstanceOf[(Params, Option[PrintWriter], Option[PrintWriter])]
    val partialEvalStats = partialOutput._1
    val trueWidget = getTrueWidget(mention)
    val predWidget = getPredWidget(mention)
    for (writer <- partialOutput._2) writer.println(toWhatsWrong(mention.words, trueWidget))
    for (writer <- partialOutput._3) writer.println(toWhatsWrong(mention.words, predWidget))
    updateEval(trueWidget, predWidget, partialEvalStats)
  }
}

class DefaultSegmentationEvaluator(val evalName: String, val inputColl: MongoCollection, val params: Params,
                                   val root: FieldCollection, val outputWhatsWrong: Boolean = false,
                                   val evalQuery: MongoDBObject = MongoDBObject("isRecord" -> false))
  extends SegmentationEvaluator {
  def name = "defaultSegmentationEvaluator"

  def getPredWidget(mention: Mention) = {
    val inferencer = new DefaultSegmentationInferencer(root, mention, params, params, SimpleInferSpec(bestUpdate = true))
    TextSegmentationHelper.adjustSegmentation(mention.words, inferencer.bestTextSegmentation)
  }
}

class ConstrainedSegmentationEvaluator(val evalName: String, val inputColl: MongoCollection, val params: Params,
                                       val constraintParams: Params, val constraintFns: Seq[ConstraintFunction],
                                       val root: FieldCollection, val outputWhatsWrong: Boolean = false,
                                       val evalQuery: MongoDBObject = MongoDBObject("isRecord" -> false))
  extends SegmentationEvaluator {
  def name = "constrainedSegmentationEvaluator"

  def getPredWidget(mention: Mention) = {
    val inferencer = new ConstrainedSegmentationInferencer(root, mention, params, params,
      constraintParams, constraintParams, constraintFns, SimpleInferSpec(bestUpdate = true))
    TextSegmentationHelper.adjustSegmentation(mention.words, inferencer.bestTextSegmentation)
  }
}

class SupervisedSegmentationOnlyLearner(val mentionColl: MongoCollection, val root: FieldCollection,
                                        val useOracle: Boolean = false) extends HasLogger {
  def learn(numIter: Int, initParams: Params = new Params, invVariance: Double = 1.0) = {
    // initialize parameters first
    val params = new DefaultParameterInitializer(root, mentionColl, initParams).run()
      .asInstanceOf[(Params, ProbStats)]._1
    params.increment(initParams, 1)
    logger.info("#parameters=" + params.numParams + " #paramGroups=" + params.size)

    // initialize constraints
    val constraints = new ParameterProcessor(root, params, useOracle) {
      def name = "recordSegmentationConstraintsInitializer"

      def inputColl = mentionColl

      def processMention(mention: Mention, partialConstraints: Params, partialStats: ProbStats) {
        val inferencer = new DefaultSegmentationInferencer(root, mention, params, partialConstraints,
          SimpleInferSpec(trueSegmentInfer = true, stepSize = 1))
        inferencer.updateCounts()
      }
    }.run().asInstanceOf[(Params, ProbStats)]._1
    // logger.info("constraints: " + constraints)

    val objective = new ACRFObjective(params, invVariance) {
      def getValueAndGradient = {
        val (expectations, stats) = new ParameterProcessor(root, params, useOracle) {
          def name = "recordSegmentationExpectationInitializer"

          def inputColl = mentionColl

          def processMention(mention: Mention, partialExpectations: Params, partialStats: ProbStats) {
            val predInferencer = new DefaultSegmentationInferencer(root, mention, params, partialExpectations,
              SimpleInferSpec(stepSize = -1))
            partialStats -= predInferencer.stats
            predInferencer.updateCounts()
          }
        }.run().asInstanceOf[(Params, ProbStats)]
        // logger.info("constraints * params=" + constraints.dot(params))
        stats.logZ += constraints.dot(params)
        // add constraints
        expectations.increment(constraints, 1)
        (expectations, stats)
      }
    }

    // optimize
    val ls = new ArmijoLineSearchMinimization
    val stop = new AverageValueDifference(1e-4)
    val optimizer = new LBFGS(ls, 4)
    val stats = new OptimizerStats {
      override def collectIterationStats(optimizer: Optimizer, objective: Objective) {
        super.collectIterationStats(optimizer, objective)
        logger.info("*** finished record segmentation learning only epoch=" + (optimizer.getCurrentIteration + 1))
      }
    }
    optimizer.setMaxIterations(numIter)
    optimizer.optimize(objective, stats, stop)

    params
  }
}

class SemiSupervisedJointSegmentationLearner(val mentionColl: MongoCollection, val root: FieldCollection,
                                             val useOracle: Boolean = true)
  extends HasLogger {
  protected def constraintLearn(numIter: Int, constraintFns: Seq[ConstraintFunction],
                                params: Params, constraintParams: Params, constraintInvVariance: Double,
                                recordWeight: Double, textWeight: Double) {
    val constraintTarget = new ParameterProcessor(root, constraintParams, useOracle) {
      def name = "semiSupConstraintTargetInitializer"

      def inputColl = mentionColl

      def processMention(mention: Mention, partialParams: Params, partialStats: ProbStats) {
        val constrStpSz = if (mention.isRecord) recordWeight else textWeight
        val inferencer = new ConstrainedSegmentationInferencer(root, mention, params, params, constraintParams,
          new Params, constraintFns, SimpleInferSpec(trueSegmentInfer = mention.isRecord, stepSize = 0, constraintStepSize = 0,
            constraintTargetStepSize = constrStpSz), partialParams)
        inferencer.updateCounts()
      }
    }.run().asInstanceOf[(Params, ProbStats)]._1
    // logger.info("constraint target:" + constraintTarget)

    val objective = new ACRFObjective(constraintParams, constraintInvVariance) {
      def getValueAndGradient = {
        val (expectations, stats) = new ParameterProcessor(root, constraintParams, useOracle) {
          def name = "semiSupConstraintExpectationInitializer"

          def inputColl = mentionColl

          def processMention(mention: Mention, partialExpectations: Params, partialStats: ProbStats) {
            val constrStpSz = if (mention.isRecord) recordWeight else textWeight
            val predInferencer = new ConstrainedSegmentationInferencer(root, mention, params, params,
              constraintParams, partialExpectations, constraintFns,
              SimpleInferSpec(stepSize = 0, constraintStepSize = -constrStpSz))
            partialStats -= predInferencer.stats * constrStpSz
            predInferencer.updateCounts()
          }
        }.run().asInstanceOf[(Params, ProbStats)]
        stats.logZ += constraintParams.dot(constraintTarget)
        // add constraints
        expectations.increment(constraintTarget, 1)
        // logger.info("gradient: " + expectations)
        (expectations, stats)
      }
    }

    val ls = new ArmijoLineSearchMinimizationAlongProjectionArc(new InterpolationPickFirstStep(1))
    val stop = new AverageValueDifference(1e-4)
    val optimizer = new ProjectedGradientDescent(ls)
    val stats = new OptimizerStats {
      override def collectIterationStats(optimizer: Optimizer, objective: Objective) {
        super.collectIterationStats(optimizer, objective)
        logger.info("*** finished constraint alignment+segmentation learning only epoch=" +
          (optimizer.getCurrentIteration + 1))
      }
    }
    optimizer.setMaxIterations(numIter)
    optimizer.optimize(objective, stats, stop)
  }

  protected def paramLearn(numIter: Int, constraintFns: Seq[ConstraintFunction],
                           params: Params, constraintParams: Params,
                           recordWeight: Double, textWeight: Double, invVariance: Double) {
    // initialize constraints
    val constraints = new ParameterProcessor(root, params, useOracle) {
      def name = "semiSupParamConstraintsInitializer"

      def inputColl = mentionColl

      def processMention(mention: Mention, partialConstraints: Params, partialStats: ProbStats) {
        val stpSz = if (mention.isRecord) recordWeight else textWeight
        val inferencer = new ConstrainedSegmentationInferencer(root, mention, params, partialConstraints,
          constraintParams, constraintParams, constraintFns,
          SimpleInferSpec(trueSegmentInfer = mention.isRecord, stepSize = stpSz))
        inferencer.updateCounts()
      }
    }.run().asInstanceOf[(Params, ProbStats)]._1

    val objective = new ACRFObjective(params, invVariance) {
      def getValueAndGradient = {
        val (expectations, stats) = new ParameterProcessor(root, params, useOracle) {
          def name = "semiSupParamExpectationsInitializer"

          def inputColl = mentionColl

          def processMention(mention: Mention, partialExpectations: Params, partialStats: ProbStats) {
            val stpSz = if (mention.isRecord) recordWeight else textWeight
            val predInferencer = new DefaultSegmentationInferencer(root, mention, params, partialExpectations,
              SimpleInferSpec(stepSize = -stpSz))
            partialStats -= predInferencer.stats * stpSz
            predInferencer.updateCounts()
          }
        }.run().asInstanceOf[(Params, ProbStats)]
        // logger.info("constraints * params=" + constraints.dot(params))
        stats.logZ += constraints.dot(params)
        // add constraints
        expectations.increment(constraints, 1)
        (expectations, stats)
      }
    }

    // optimize
    val ls = new ArmijoLineSearchMinimization
    val stop = new AverageValueDifference(1e-4)
    val optimizer = new LBFGS(ls, 4)
    val stats = new OptimizerStats {
      override def collectIterationStats(optimizer: Optimizer, objective: Objective) {
        super.collectIterationStats(optimizer, objective)
        logger.info("*** finished joint segmentation learning epoch=" + (optimizer.getCurrentIteration + 1))
      }
    }
    optimizer.setMaxIterations(numIter)
    optimizer.optimize(objective, stats, stop)
  }

  def learn(numIter: Int = 5, numConstraintIter: Int = 50, numParamIter: Int = 50,
            constraintFns: Seq[ConstraintFunction] = Seq.empty[ConstraintFunction],
            initParams: Params = new Params, initConstraintParams: Params = new Params,
            invVariance: Double = 1, constraintInvVariance: Double = 0,
            recordWeight: Double = 1, textWeight: Double = 1e-2) = {
    // initialize parameters first
    val params = new DefaultParameterInitializer(root, mentionColl, initParams).run()
      .asInstanceOf[(Params, ProbStats)]._1
    params.increment(initParams, 1)
    logger.info("#parameters=" + params.numParams + " #paramGroups=" + params.size)
    val constraintParams = new ConstrainedParameterInitializer(root, mentionColl, params,
      initConstraintParams, constraintFns).run().asInstanceOf[(Params, ProbStats)]._1
    constraintParams.increment(initConstraintParams, 1)
    logger.info("#constraintParameters=" + constraintParams.numParams + " #constraintParamGroups=" + constraintParams.size)

    for (iter <- 1 to numIter) {
      logger.info("starting outer iteration=" + iter)

      paramLearn(numParamIter, constraintFns, params, constraintParams, recordWeight, textWeight, invVariance)
      val paramEvalStats = new ConstrainedSegmentationEvaluator("semi-sup-segmentation-iteration-" + iter, mentionColl,
        params, constraintParams, constraintFns, root).run()
        .asInstanceOf[(Params, Option[PrintWriter], Option[PrintWriter])]._1
      TextSegmentationHelper.outputEval("semi-sup-segmentation-after-param-iteration-" + iter,
        paramEvalStats, logger.info(_))

      constraintLearn(numConstraintIter, constraintFns, params, constraintParams, constraintInvVariance, recordWeight, textWeight)
      // logger.info("constraintParams:" + constraintParams)
      val constraintEvalStats = new ConstrainedSegmentationEvaluator("semi-sup-segmentation-iteration-" + iter, mentionColl,
        params, constraintParams, constraintFns, root).run()
        .asInstanceOf[(Params, Option[PrintWriter], Option[PrintWriter])]._1
      TextSegmentationHelper.outputEval("semi-sup-segmentation-after-constraint-iteration-" + iter,
        constraintEvalStats, logger.info(_))
    }

    (params, constraintParams)
  }
}