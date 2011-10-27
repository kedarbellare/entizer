package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._
import akka.actor.Actor._
import akka.util.duration._
import akka.actor.{Channel, Actor, PoisonPill}
import akka.routing.{Routing, CyclicIterator}
import Routing._
import org.riedelcastro.nurupo.HasLogger

import System.{currentTimeMillis => now}

/**
 * @author kedar
 */

object JobCenter {

  case class Job(query: DBObject = MongoDBObject(), select: DBObject = MongoDBObject(),
                 skip: Int = 0, limit: Int = Int.MaxValue, inputParams: Any = null) {
    override def toString = "[[query=" + query + ", select=" + select + ", skip=" + skip + ", limit=" + limit + "]]"
  }

  case class Work(dbo: DBObject, inputParams: Any = null)

  case class WorkResult(partialOutputParams: Any = null)

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
    protected def receive = {
      case work: Work => {
        // initialize partial output
        val partialOutputParams = newOutputParams()
        // process the work
        process(work.dbo, work.inputParams, partialOutputParams)
        // reply to master
        self reply WorkResult(partialOutputParams)
      }
    }
  }

  class Master extends Actor with HasLogger {
    val nrOfWorkers: Int = math.min(Conf.get[Int]("max-workers", 1), Runtime.getRuntime.availableProcessors())
    var nrOfMessages: Int = 0
    var nrOfResults: Int = 0
    val outputParams = newOutputParams(true)

    // create the workers
    val workers = Vector.fill(nrOfWorkers)(actorOf(new Worker).start())

    // wrap in load-balancing router
    val router = Routing.loadBalancerActor(CyclicIterator(workers)).start()

    // phase 1: scatter messages
    def scatter: Receive = {
      case job: Job => {
        // schedule job by iterating over items
        logger.info("Master received job: " + job)
        nrOfMessages = inputColl.find(job.query, job.select).skip(job.skip).limit(job.limit).count
        val dboIter = inputColl.find(job.query, job.select).batchSize(5000).skip(job.skip).limit(job.limit)
        while (dboIter.hasNext) router ! Work(dboIter.next(), job.inputParams)
        logger.info("Master scattered all objects: #messages=" + nrOfMessages + "; changing to gatherer")

        this become gather(self.channel)
      }
    }

    // phase 2: gather results
    def gather(recipient: Channel[Any]): Receive = {
      case result: WorkResult => {
        // merge output params with partial
        merge(outputParams, result.partialOutputParams)
        // increment #results
        nrOfResults += 1
        if (nrOfResults % debugEvery == 0 || nrOfResults == nrOfMessages) {
          val percentComplete: Float = (100.0f * nrOfResults) / nrOfMessages
          logger.info("Master received #results=" + nrOfResults + "/" + nrOfMessages +
            "; Completed=" + "%.1f".format(percentComplete) + "%")
        }
        if (nrOfResults == nrOfMessages) {
          // send final output params to original job submitter
          recipient ! JobResult(outputParams)
          // shut down
          self.stop()
        }
      }
    }

    // message handler starts at the scattering behavior
    protected def receive = scatter

    // when we are stopped, stop our team of workers and our router
    override def postStop() {
      // send a PoisonPill to all workers telling them to shut down themselves
      router ! Broadcast(PoisonPill)
      // send a PoisonPill to the router, telling him to shut himself down
      router ! PoisonPill
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

  def run() = {
    preRun()

    // create master
    val master = actorOf(new Master).start()

    // start the job
    val start = now

    // send job to master
    val outputParams = master.?(inputJob)(timeout = Actor.Timeout(30 days)).await.resultOrException match {
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