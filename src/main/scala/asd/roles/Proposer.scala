package asd.roles

import asd.messages._

import akka.actor.{Actor, ActorRef, Props, ReceiveTimeout}
import akka.event.{Logging, LoggingAdapter}
import akka.util.Timeout

import scala.concurrent.duration._
import scala.collection.parallel.mutable.ParHashMap

class Proposer(acceptors: List[ActorRef], learners: List[ActorRef], num_replicas: Int, quorum: Int, index: Int) extends Actor {

  val timeout = Timeout(1000 milliseconds)
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

  class PutRunner(key: String, value: String) extends Actor {

    var client: ActorRef = null

    def waiting_for_accept_ok(received: Int, highest_n: Int, final_va: String, replicated_acceptors: List[ActorRef]): Receive = {
      case AcceptOk(`key`, `highest_n`) => {
        if (received + 1 == quorum) {
          replicated_acceptors.par.foreach(_ ! Decided(key, final_va))
          client ! Ack
          log.info("Key: {}, Value: {}", key, final_va)
        }

        context.become(waiting_for_accept_ok(received + 1, highest_n, final_va, replicated_acceptors))
      }
      case ReceiveTimeout => {
        // TODO: maybe
        if (received < quorum) log.warning("Timeout on AcceptOk. Was: {}, Received: {}", self, received)
        // respond_to ! Timedout
        context.stop(self)
      }
      case Stop => context.stop(self)
    }

    def waiting_for_prepare_ok(received: Int, highest_n: Int, v: String, na: Int, va: String, replicated_acceptors: List[ActorRef]): Receive = {
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
          context.become(waiting_for_accept_ok(0, highest_n, final_va, replicated_acceptors))
        } else {
          if (na_ok > na) {
            context.become(waiting_for_prepare_ok(received + 1, highest_n, v, na_ok, va_ok, replicated_acceptors))
          } else {
            context.become(waiting_for_prepare_ok(received + 1, highest_n, v, na, va, replicated_acceptors))
          }
        }
      }
      case PrepareTooLow(`key`, n) => {
        n_store.put(key, n)
        log.info("Prepare too low. Key: {}, Value: {}", key, value)
        //context.stop(self)
        //self ! Stop
      }
      case ReceiveTimeout => {
        // TODO: maybe
        // log.warning("Timeout on replication. Was: {}, Received: {}, Result: {}", self, received, result)
        // respond_to ! Timedout
        context.stop(self)
      }
      case Stop => context.stop(self)
    }

    def receive = {
      case StartRun(c) => {
        client = c
        val highest_n = n_store.get(key) match {
          case Some(v) => v + index + 1
          case None  => 0 + index + 1
        }
        n_store.put(key, highest_n)
        val replicated_acceptors = pick_replicas(key, acceptors)

        replicated_acceptors.par.foreach(_ ! Prepare(key, highest_n))
        context.setReceiveTimeout(timeout.duration)
        context.become(waiting_for_prepare_ok(0, highest_n, value, -1, null, replicated_acceptors))
      }
    }
  }

  class GetRunner(key: String) extends Actor {

    var client: ActorRef = null

    def waiting_for_learner_result(): Receive = {
      case result @ Result(`key`, v) => {
        client ! result
        self ! Stop
      }
      case ReceiveTimeout => {
        log.warning("Proposer: Timeout on Get({}) | Was: {}", key, self)
        context.stop(self)
      }
      case Stop => context.stop(self)
    }

    def receive = {
      case StartRun(c) => {
        client = c
        learners(index) ! Get(key)
        context.setReceiveTimeout(timeout.duration)
        context.become(waiting_for_learner_result())
      }
    }
  }

  def receive = {
    case Put(key, value) => {
      log.info("{} {}", key, value)
      val runner = context.actorOf(Props(new PutRunner(key, value)))
      runner ! StartRun(sender)
    }
    case Get(key) => {
      val runner = context.actorOf(Props(new GetRunner(key)))
      runner ! StartRun(sender)
    }
  }
}