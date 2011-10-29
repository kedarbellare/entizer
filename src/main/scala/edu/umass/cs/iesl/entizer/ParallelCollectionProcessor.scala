package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._
import akka.actor.Actor._
import akka.util.duration._
import akka.routing.{Routing, CyclicIterator}
import Routing._
import org.riedelcastro.nurupo.HasLogger

import System.{currentTimeMillis => now}
import akka.dispatch.Dispatchers
import akka.actor._

/**
 * @author kedar
 */

object JobCenter {

  case class Job(query: DBObject = MongoDBObject(), select: DBObject = MongoDBObject(),
                 skip: Int = 0, limit: Int = Int.MaxValue, inputParams: Any = null) {
    override def toString = "[[query=" + query + ", select=" + select + ", skip=" + skip + ", limit=" + limit + "]]"
  }

  case object JobStart

  case class Work(dbo: DBObject, inputParams: Any = null)

  case object WorkDone

  case object JobDone

  case class JobResult(outputParams: Any = null)

}

trait ParallelCollectionProcessor extends HasLogger {

  import JobCenter._

  def name: String

  def parallelName: String = "parallel[" + name + "]"

  def debugEvery: Int = 1000

  // input collection
  def inputColl: MongoCollection

  def preRun() {
    logger.info("")
    logger.info("Starting " + parallelName)
  }

  // adapted from akka-tutorial scala (part 2):
  // https://github.com/jboner/akka/blob/release-1.2/akka-tutorials/akka-tutorial-second/src/main/scala/Pi.scala
  class Worker extends Actor with HasLogger {
    self.dispatcher = Dispatchers.newThreadBasedDispatcher(self)

    // initialize partial output
    val partialOutputParams = newOutputParams()

    protected def receive = {
      case work: Work => {
        // process the work
        process(work.dbo, work.inputParams, partialOutputParams)
        // reply to master
        self reply WorkDone
      }
      case JobDone => {
        self reply JobResult(partialOutputParams)
        self.stop()
      }
    }
  }

  class Master extends Actor with HasLogger {
    val nrOfWorkers: Int = numWorkers
    var nrOfMessages: Int = 0
    var nrOfDones: Int = 0
    var nrOfResults: Int = 0
    val inputParams = inputJob.inputParams
    val dboIter = inputColl.find(inputJob.query, inputJob.select).skip(inputJob.skip).limit(inputJob.limit).toIterator
    val outputParams = newOutputParams(true)

    // create the workers
    val workers = Vector.fill(nrOfWorkers)(actorOf(new Worker).start())

    // original recipient
    var origRecipient: Option[Channel[Any]] = None

    // phase 1: scatter messages
    protected def receive = {
      case JobStart => {
        // schedule job by iterating over items
        logger.info("Master starting job: " + inputJob)
        origRecipient = Some(self.channel)
        for (worker <- workers) {
          if (dboIter.hasNext) {
            worker ! Work(dboIter.next(), inputParams)
            nrOfMessages += 1
          }
          else worker ! JobDone
        }
      }
      case WorkDone => {
        nrOfDones += 1
        if (nrOfDones % debugEvery == 0) {
          logger.info("Master received #dones=" + nrOfDones + "/#messages=" + nrOfMessages)
        }
        if (dboIter.hasNext) {
          self reply Work(dboIter.next(), inputParams)
          nrOfMessages += 1
        } else {
          self reply JobDone
        }
      }
      case result: JobResult => {
        // merge output params with partial
        merge(outputParams, result.outputParams)
        logger.info("Master received #dones=" + nrOfDones + "/#messages=" + nrOfMessages)
        // increment #results
        nrOfResults += 1
        logger.info("Master received #result=" + nrOfResults + "/#workers=" + nrOfWorkers)
        if (nrOfResults == nrOfWorkers) {
          require(origRecipient.isDefined)
          origRecipient.get ! JobResult(outputParams)
          self.stop()
        }
      }
    }
  }

  // view on which to run process
  def inputJob: Job

  // new output parameters (for master/worker)
  def newOutputParams(isMaster: Boolean = false): Any = null

  // actual work: should be atomic and not change global data structures
  def process(dbo: DBObject, inputParams: Any, partialOutputParams: Any)

  // merge partial output parameters
  def merge(outputParams: Any, partialOutputParams: Any) {}

  // number of workers (can be overridden say for thread-unsafe processing)
  def numWorkers: Int = math.min(Conf.get[Int]("max-workers", 1), Runtime.getRuntime.availableProcessors())

  def run() = {
    preRun()

    // create master
    val master = actorOf(new Master).start()

    // start the job
    val start = now

    // send job to master
    val outputParams = master.?(JobStart)(timeout = Actor.Timeout(30 days)).await.resultOrException match {
      // wait for result with a long timeout!! (30 days!!!)
      case Some(result) => {
        logger.info("Completed " + parallelName + " in time=" + (now - start) + " millis")
        result.asInstanceOf[JobResult].outputParams
      }
      case None => {
        logger.error("Failed to complete " + parallelName + " after time=" + (now - start) + " millis")
        throw new RuntimeException("Job " + inputJob + " -> " + parallelName + " failed!!!")
      }
    }

    // return merged output parameters
    outputParams
  }
}