package asd.roles

import asd.messages._

import akka.actor.{Actor, ActorRef}
import akka.event.{Logging, LoggingAdapter}

import scala.collection.parallel.mutable.ParHashMap

class Acceptor(learners: List[ActorRef], index: Int) extends Actor {

  // np, na, va
  var state_store = new ParHashMap[Int, (Int, Int, ActorRef)]
  val log = Logging.getLogger(context.system, this)

  def receive = {
    case PrepareLeader(idx, n) => {
      state_store.get(idx) match {
        case Some((np, na, va)) => {
          if (n > np) {
            state_store.put(idx, (n, na, va))
            sender ! PrepareLeaderOk(idx, na, va)
          } else {
            sender ! PrepareLeaderTooLow(idx, np)
          }
        }
        case None => {
          state_store.put(idx, (n, -1, null))
          sender ! PrepareLeaderOk(idx, -1, null)
        }
      }
    }
    case AcceptLeader(idx, n, value) => {
      state_store.get(idx) match {
        case Some((np, _, _)) => {
          if (n >= np) {
            state_store.put(idx, (np, n, value))
            sender ! AcceptLeaderOk(idx, n)
          }
        }
        case _ => {
          log.warning("Accept({}, {}, {}) did not find the key in the state_store.", idx, n, value)
        }
      }
    }
    case Stop => context.stop(self)
  }
}