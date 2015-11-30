package asd.clients

import asd.messages._
import asd.roles.Proposer

import akka.actor.{Actor, ActorRef, ReceiveTimeout}
import akka.event.{Logging, LoggingAdapter}
import akka.util.Timeout

import scala.concurrent.duration._
import scala.collection.parallel.mutable.ParHashMap
import scala.collection.mutable.HashSet

class Client(replicas: Vector[Vector[ActorRef]], num_replicas: Int, quorum: Int) extends Actor {

  val timeout = Timeout(2000 milliseconds)
  val log = Logging.getLogger(context.system, this)

  var operations: Double = 0
  var latency: Double = 0

  var stopped_responding = new HashSet[ActorRef]

  def pick_replicas(key: String): List[ActorRef] = {
    val idx = Math.abs(key.hashCode() % num_replicas)

    replicas(idx).toList
  }

  def waiting_for_ack(respond_to: ActorRef, key: String, value: String, start_time: Double, replica: ActorRef): Receive = {
    case Ack => {
      val end_time = System.nanoTime.toDouble
      operations += 1
      latency += (end_time - start_time)

      log.info("Finished Put({}, {})", key, value)

      respond_to ! Ack
      context.become(receive)
    }
    case ReceiveTimeout => {
      val end_time = System.nanoTime.toDouble
      operations += 1
      latency += (end_time - start_time)
      stopped_responding.add(replica)

      log.warning("Timeout on Put({}, {})", key, value)
      context.become(receive)
    }
  }

  def waiting_for_result(respond_to: ActorRef, key: String, start_time: Double, replica: ActorRef): Receive = {
    case result @ Result(_, _) => {
      val end_time = System.nanoTime.toDouble
      operations += 1
      latency += (end_time - start_time)

      log.info("Finished Get({}), result={}", key, result)

      respond_to ! result
      context.become(receive)
    }
    case ReceiveTimeout => {
      val end_time = System.nanoTime.toDouble
      operations += 1
      latency += (end_time - start_time)
      stopped_responding.add(replica)

      log.warning("Timeout on Get({})", key)
      context.become(receive)
    }
  }

  def receive = {
    case Put(key, value) => {
      log.info("Begin Put({}, {})", key, value)
      val start_time = System.nanoTime.toDouble
      pick_replicas(key).find(!stopped_responding.contains(_)) match {
        case Some(r) => {
          r ! CPut(self, key, value)
          context.setReceiveTimeout(timeout.duration)
          context.become(waiting_for_ack(sender, key, value, start_time, r))
        }
        case None => log.warning("No replicas for key {} are responding.", key)
      }
    }
    case Get(key) => {
      log.info("Begin Get({})", key)
      val start_time = System.nanoTime.toDouble
      pick_replicas(key).find(!stopped_responding.contains(_)) match {
        case Some(r) => {
          r ! CGet(self, key)
          context.setReceiveTimeout(timeout.duration)
          context.become(waiting_for_result(sender, key, start_time, r))
        }
        case None => log.warning("No replicas for key {} are responding.", key)
      }
    }
    case Stop => {
      // convert from nanoseconds to milliseconds
      sender ! AvgLatency(latency / 1000000 / operations)
    }
  }
}