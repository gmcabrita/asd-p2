package asd.roles

import asd.messages._

import akka.actor.{Actor, ActorRef, ReceiveTimeout}
import akka.event.{Logging, LoggingAdapter}
import akka.util.Timeout

import scala.concurrent.duration._
import scala.collection.parallel.mutable.ParHashMap

class Proposer(acceptors: List[ActorRef], learners: List[ActorRef], num_replicas: Int, quorum: Int, index: Int) extends Actor {

  val timeout = Timeout(15 milliseconds)
  val log = Logging.getLogger(context.system, this)

  // store for the highest n of each key
  var n_store = new ParHashMap[String, Int]

  def pick_replicas(key: String, servers: List[ActorRef]): List[ActorRef] = {
    val start = Math.abs(key.hashCode() % num_replicas)

    val picked = servers.slice(start, start + num_replicas)
    if (picked.size < num_replicas) {
      picked ++ servers.slice(0, num_replicas - picked.size)
    } else {
      picked
    }
  }

  def waiting_for_learner_result(respond_to: ActorRef, key: String): Receive = {
    case Result(`key`, v) => {
      respond_to ! v
      context.become(receive)
    }
    case ReceiveTimeout => {
      log.warning("Timeout on Get({}) | Was: {}", key, self)
      context.become(receive)
    }
  }

  def waiting_for_accept_ok(respond_to: ActorRef, received: Int, highest_n: Int, key: String, final_va: String, replicated_acceptors: List[ActorRef]): Receive = {
    case AcceptOk(`key`, `highest_n`) => {
      if (received + 1 == quorum) {
        // send decided(key, final_va) to replicated acceptors and reply to respond_to
        replicated_acceptors.par.foreach(_ ! Decided(key, final_va))
        respond_to ! Ack()
        context.become(receive)
      }

      context.become(waiting_for_accept_ok(respond_to, received + 1, highest_n, key, final_va, replicated_acceptors))
    }
    case ReceiveTimeout => {
      // TODO: maybe
      // log.warning("Timeout on replication. Was: {}, Received: {}, Result: {}", self, received, result)
      // respond_to ! Timedout
      context.become(receive)
    }
  }

  def waiting_for_prepare_ok(respond_to: ActorRef, received: Int, highest_n: Int, key: String, v: String, na: Int, va: String, replicated_acceptors: List[ActorRef]): Receive = {
    case PrepareOk(`key`, na_ok, va_ok) => {
      if (received + 1 == quorum) {
        // TODO: make this less weird
        val step_nva = if (na_ok > na) {
          (na_ok, va_ok)
        } else {
          (na, va)
        }

        val final_va = if (step_nva._1 > -1) {
          step_nva._2
        } else {
          v
        }

        replicated_acceptors.par.foreach(_ ! Accept(key, highest_n, final_va))
        context.setReceiveTimeout(timeout.duration)
        context.become(waiting_for_accept_ok(respond_to, 0, highest_n, key, final_va, replicated_acceptors))
      }

      if (na_ok > na) {
        context.become(waiting_for_prepare_ok(respond_to, received + 1, highest_n, key, v, na_ok, va_ok, replicated_acceptors))
      } else {
        context.become(waiting_for_prepare_ok(respond_to, received + 1, highest_n, key, v, na, va, replicated_acceptors))
      }
    }
    case PrepareTooLow(`key`, n) => {
      n_store.put(key, n)
    }
    case ReceiveTimeout => {
      // TODO: maybe
      // log.warning("Timeout on replication. Was: {}, Received: {}, Result: {}", self, received, result)
      // respond_to ! Timedout
      context.become(receive)
    }
  }

  def receive = {
    case Put(key, value) => {
      val highest_n = n_store.get(key) match {
        case Some(v) => v + 1
        case None  => 0 + 1
      }
      n_store.put(key, highest_n)
      val replicated_acceptors = pick_replicas(key, acceptors)

      replicated_acceptors.par.foreach(_ ! Prepare(key, highest_n))
      context.setReceiveTimeout(timeout.duration)
      context.become(waiting_for_prepare_ok(sender, 0, highest_n, key, value, -1, null, replicated_acceptors))
    }
    case Get(key) => {
      // ask our related learner, this should never time out because our learner should always be up if we (this proposer) are up
      learners(index) ! Get(key)
      context.setReceiveTimeout(timeout.duration)
      context.become(waiting_for_learner_result(sender, key))
    }
  }
}