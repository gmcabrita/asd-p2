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

  def pick_replicas(key: String, servers: List[ActorRef]): List[ActorRef] = {
    val start = Math.abs(key.hashCode() % num_replicas)

    val picked = servers.slice(start, start + num_replicas)
    if (picked.size < num_replicas) {
      picked ++ servers.slice(0, num_replicas - picked.size)
    } else {
      picked
    }
  }

  def waiting_for_ack(respond_to: ActorRef, key: String, value: String, start_time: Long): Receive = {
    case Ack => {
      val end_time = System.nanoTime
      operations += 1
      latency += (end_time - start_time)

      // TODO: remove
      log.info("Finished Put({}, {})", key, value)

      respond_to ! Ack
      context.become(receive)
    }
    case ReceiveTimeout => {
      val end_time = System.nanoTime
      operations += 1
      latency += (end_time - start_time)

      log.warning("Timeout on Put({}, {}) | Was: {}", key, value, self)
      context.become(receive)
    }
  }

  def waiting_for_result(respond_to: ActorRef, key: String, start_time: Long): Receive = {
    case result @ Result(_, _) => {
      val end_time = System.nanoTime
      operations += 1
      latency += (end_time - start_time)

      // TODO: remove
      log.info("Finished Get({}), result={}", key, result)

      respond_to ! result
      context.become(receive)
    }
    case ReceiveTimeout => {
      val end_time = System.nanoTime
      operations += 1
      latency += (end_time - start_time)

      log.warning("Timeout on Get({}) | Was: {}", key, self)
      context.become(receive)
    }
  }

  def receive = {
    case Put(key, value) => {
      log.info("Begin Put({}, {})", key, value)
      val start_time = System.nanoTime
      log.info("{}", pick_replicas(key, proposers))
      pick_replicas(key, proposers).par.foreach(_ ! Put(key, value))
      context.setReceiveTimeout(timeout.duration)
      context.become(waiting_for_ack(sender, key, value, start_time))
    }
    case Get(key) => {
      val start_time = System.nanoTime
      pick_replicas(key, proposers).par.foreach(_ ! Get(key))
      context.setReceiveTimeout(timeout.duration)
      context.become(waiting_for_result(sender, key, start_time))
    }
    case Stop => {
      log.info("Client {}, average latency: {}", self, latency / operations)
    }
  }
}