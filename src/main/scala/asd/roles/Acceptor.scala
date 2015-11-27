package asd.roles

import asd.messages._

import akka.actor.{Actor, ActorRef}
import akka.event.{Logging, LoggingAdapter}

import scala.collection.parallel.mutable.ParHashMap

class Acceptor(learners: List[ActorRef], index: Int) extends Actor {

  // np, na, va
  var state_store = new ParHashMap[String, (Int, Int, String)]
  val log = Logging.getLogger(context.system, this)

  def receive = {
    case Prepare(key, n) => {
      state_store.get(key) match {
        case Some((np, na, va)) => {
          if (n > np) {
            state_store.put(key, (n, na, va))
            sender ! PrepareOk(key, na, va)
          } else {
            sender ! PrepareTooLow(key, np)
          }
        }
        case None => {
          state_store.put(key, (n, -1, null))
          sender ! PrepareOk(key, -1, null)
        }
      }
    }
    case Accept(key, n, value) => {
      state_store.get(key) match {
        case Some((np, _, _)) => {
          if (n >= np) {
            state_store.put(key, (np, n, value))
            sender ! AcceptOk(key, n)
          }
        }
        case _ => {
          log.warning("Accept({}, {}, {}) did not find the key in the state_store.", key, n, value)
        }
      }
    }
    case Decided(key, value) => {
      //state_store.remove(key)
      learners(index) ! Decided(key, value)
    }
  }
}