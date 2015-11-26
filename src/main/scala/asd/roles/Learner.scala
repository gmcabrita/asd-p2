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
      log.info("stored: {}, {}", key, value)
      store.put(key, value)
    }
    case Get(key) => {
      val value = store.get(key) match {
        case v @ Some(va) => {
          //log.info("k: {}, v: {}", key, va)
          Result(key, v)
        }
        case None => {
          //log.info("k: {}, v: None", key)
          Result(key, None)
        }
      }
      sender ! value
    }
  }
}