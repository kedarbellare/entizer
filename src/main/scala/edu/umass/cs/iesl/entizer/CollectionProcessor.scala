package edu.umass.cs.iesl.entizer

import com.mongodb.casbah.Imports._
import org.riedelcastro.nurupo.HasLogger

/**
 * @author kedar
 */


trait CollectionProcessor extends HasLogger {
  def name: String
  
  // collection to be modified
  def inputColl: MongoCollection

  def preRun() {
    logger.info("")
    logger.info("Starting " + name)
  }

  // main method to call
  def run()
}