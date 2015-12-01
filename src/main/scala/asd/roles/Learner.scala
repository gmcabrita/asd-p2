package asd.roles

import asd.messages._

import akka.actor.{Actor, ActorRef}
import akka.event.{Logging, LoggingAdapter}

import scala.collection.parallel.mutable.ParHashMap

class Learner extends Actor {

  val log = Logging.getLogger(context.system, this)
  var store = new ParHashMap[String, String]

  def receive = {
    case Decided(key, value) => {
      log.info("Stored: {}, {}", key, value)
      store.put(key, value)

      sender ! Stored
    }
    case Get(key) => {
      val value = store.get(key) match {
        case v @ Some(va) => {
          Result(key, v)
        }
        case None => {
          Result(key, None)
        }
      }
      log.info("Read: {}, {}", key, value)
      sender ! value
    }
    case Stop => context.stop(self)
  }
}