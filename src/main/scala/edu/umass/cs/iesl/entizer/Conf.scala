package edu.umass.cs.iesl.entizer

import java.io.InputStream
import org.riedelcastro.nurupo._

/**
 * @author kedar
 */

object Conf extends Config(ConfUtil.getConfStream())

object ConfUtil extends HasLogger {

  import java.net._

  def getConfStream(name: String = "entizer-" + userName() + "@" + short()): InputStream = {
    if (!name.contains("-"))
      Util.getStreamFromFileOrClassPath(name + ".properties")
    else try {
      Util.getStreamFromFileOrClassPath(name + ".properties")
    } catch {
      case e =>
        logger.info("Cannot find %s.properties, backing-off".format(name))
        //try with username

        try {
          getConfStream(name.substring(0, name.lastIndexOf("-")))
        } catch {
          case e1 =>
            //try without username
            if (name.contains("@")) {
              val atIndex = name.lastIndexOf("@")
              val firstDashIndex = name.indexOf("-")
              val removed = name.take(firstDashIndex + 1) ++ name.substring(atIndex + 1)
              getConfStream(removed)
            } else throw e1
        }


      //try without username

    }
  }

  def userName() = {
    System.getProperty("user.name");
  }

  def long() = {
    try {
      InetAddress.getLocalHost.getHostName
    } catch {
      case _ => "unknown"
    }
  }

  def short() = {
    val result = try {
      InetAddress.getLocalHost.getHostName
    } catch {
      case _ => "unknown"
    }
    val lastDot = result.indexOf(".")
    if (lastDot < 0) result else result.substring(0, lastDot)
  }

}
