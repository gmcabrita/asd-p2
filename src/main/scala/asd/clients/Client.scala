package asd.clients

import asd.messages._
import asd.roles.Proposer

import akka.actor.{Actor, ActorRef, ReceiveTimeout}
import akka.event.{Logging, LoggingAdapter}
import akka.util.Timeout

import scala.concurrent.duration._
import scala.collection.parallel.mutable.ParHashMap

class Client(proposers: List[ActorRef], num_replicas: Int, quorum: Int) extends Actor {

  val timeout = Timeout(10000 milliseconds)
  val log = Logging.getLogger(context.system, this)

  var operations: Long = 0
  var latency: Long = 0
  var start_time: Long = 0
  var evaluator: ActorRef = null
  var ckey = ""

  def pick_replicas(key: String, servers: List[ActorRef]): List[ActorRef] = {
    val start = Math.abs(key.hashCode() % num_replicas)

    val picked = servers.slice(start, start + num_replicas)
    if (picked.size < num_replicas) {
      picked ++ servers.slice(0, num_replicas - picked.size)
    } else {
      picked
    }
  }

  def receive = {
    case Put(key, value) => {
      evaluator = sender
      log.info("Begin Put({}, {}) with {}", key, value, self)
      ckey = key
      start_time = System.nanoTime
      pick_replicas(key, proposers).par.foreach(_ ! Put(key, value))
    }
    case Ack => {
      val end_time = System.nanoTime
      operations += 1
      latency += (end_time - start_time)

      log.info("Finished Get({})", ckey)
      evaluator ! Ack
    }
    case Get(key) => {
      evaluator = sender
      log.info("Begin Get({})", key)
      start_time = System.nanoTime
      pick_replicas(key, proposers).par.foreach(_ ! Get(key))
    }
    case result @ Result(key, _) => {
      val end_time = System.nanoTime
      operations += 1
      latency += (end_time - start_time)

      log.info("Finished Get({}), result={}", key, result)

      evaluator ! result
    }
    case Stop => {
      log.info("Client {}, average latency: {}", self, latency / operations)
    }
  }
}