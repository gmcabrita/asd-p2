package asd.roles

import asd.messages._

import akka.actor.{Actor, ActorRef, Props, ReceiveTimeout}
import akka.event.{Logging, LoggingAdapter}
import akka.util.Timeout
import akka.pattern.{ask, pipe}

import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.collection.parallel.mutable.ParHashMap
import scala.collection.mutable.HashSet
import scala.util.{Success, Failure}

class Proposer(learns: Vector[ActorRef], num_replicas: Int, num_faults: Int, quorum: Int, index: Int) extends Actor {

  implicit val timeout = Timeout(3000 milliseconds)

  val log = Logging.getLogger(context.system, this)

  var n_store = new ParHashMap[Int, Int]

  var leaders = new ParHashMap[Int, ActorRef]

  var acceptors: Vector[Vector[ActorRef]] = Vector()
  var proposers: Vector[Vector[ActorRef]] = Vector()
  var learners: Vector[Vector[ActorRef]] = Vector()

  def pick_replicas(key: String, servers: Vector[Vector[ActorRef]]): Vector[ActorRef] = servers(get_idx(key))
  def get_idx(key: String): Int = Math.abs(key.hashCode() % num_replicas)

  class ElectionRunner(idx: Int) extends Actor {

    var working_proposers = new HashSet[ActorRef]

    def waiting_for_accept_leader_ok(reply_to: ActorRef, received: Int, highest_n: Int, final_va: ActorRef, replicated_acceptors: List[ActorRef]): Receive = {
      case AcceptLeaderOk(`idx`, `highest_n`) => {
        if (received + 1 == quorum) {
          proposers.par.foreach(l => l.par.foreach(p => p ! DecidedLeader(idx, final_va)))
          reply_to ! Ack
        }

        context.become(waiting_for_accept_leader_ok(reply_to, received + 1, highest_n, final_va, replicated_acceptors))
      }
      case ReceiveTimeout => {
        context.stop(self)
      }
    }

    def waiting_for_prepare_leader_ok(reply_to: ActorRef, received: Int, highest_n: Int, v: ActorRef, na: Int, va: ActorRef, replicated_acceptors: List[ActorRef]): Receive = {
      case PrepareLeaderOk(`idx`, na_ok, va_ok) => {
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

          replicated_acceptors.par.foreach(_ ! AcceptLeader(idx, highest_n, final_va))
          context.setReceiveTimeout(timeout.duration)
          context.become(waiting_for_accept_leader_ok(reply_to, 0, highest_n, final_va, replicated_acceptors))
        } else {
          if (na_ok > na) {
            context.become(waiting_for_prepare_leader_ok(reply_to, received + 1, highest_n, v, na_ok, va_ok, replicated_acceptors))
          } else {
            context.become(waiting_for_prepare_leader_ok(reply_to, received + 1, highest_n, v, na, va, replicated_acceptors))
          }
        }
      }
      case PrepareLeaderTooLow(`idx`, n) => {
        n_store.put(idx, n)
      }
      case ReceiveTimeout => {
        context.stop(self)
      }
    }

    def waiting_for_pong(reply_to: ActorRef): Receive = {
      case Pong => {
        working_proposers.add(sender)
        if (working_proposers.size >= num_replicas) self ! Continue
      }
      case Continue => {
        val highest_n = n_store.get(idx) match {
          case Some(v) => v + index + 1
          case None  => 0 + index + 1
        }
        n_store.put(idx, highest_n)
        val replicated_acceptors = acceptors(idx).toList

        replicated_acceptors.par.foreach(_ ! PrepareLeader(idx, highest_n))

        proposers(idx).find(working_proposers.contains(_)) match {
          case Some(value) => {
            context.setReceiveTimeout(timeout.duration)
            context.become(waiting_for_prepare_leader_ok(reply_to, 0, highest_n, value, -1, null, replicated_acceptors))
          }
          case None => log.warning("No working proposers.")
        }

      }
      case ReceiveTimeout => self ! Continue
    }

    def receive = {
      case StartElection(reply_to) => {
        proposers(idx).par.foreach(_ ! Ping)
        context.setReceiveTimeout(timeout.duration)
        context.become(waiting_for_pong(reply_to))
      }
    }
  }

  def receive = {
    case CPut(client, key, value) => {
      val idx = get_idx(key)
      leaders.get(idx) match {
        case Some(l) => {
          if (l == self) {
            val results: Vector[Future[Any]] = pick_replicas(key, learners).map(c => ask(c, Decided(key, value)))

            // results.foreach(f => f onComplete {
            //   case Success(_) => {
            //     log.info("Received reply on Put({}, {})", key, value)
            //   }
            //   case Failure(v) => {
            //     if (num_faults == 0) {
            //       log.warning("Lost reply on Put({}, {} with Future error: {})", key, value, v)
            //       Thread.sleep(1000)
            //       sys.exit(0)
            //     }
            //   }
            // })

            val receives: Int = results.par.map(f => {
              Await.ready(f, 1 second).value.get match {
                case Success(_) => 1
                case Failure(_) => 0
              }
            }).reduce(_ + _)

            if (receives < quorum) {
              log.warning("Not enough replicas are available on Put({}, {}).", key, value)
              Thread.sleep(1000)
              sys.exit(0)
            }

            client ! Ack
          } else {
            l ! CPut(client, key, value)
          }
        }
        case None => log.warning("No leader exists for replica set starting on index {}", idx)
      }
    }
    case CGet(client, key) => {
      val idx = get_idx(key)
      leaders.get(idx) match {
        case Some(l) => {
          if (l == self) {
            val result: Future[Result] = ask(learns(index), Get(key)).mapTo[Result]
            result pipeTo client
          } else {
            l ! CGet(client, key)
          }
        }
        case None => log.warning("No leader exists for replica set starting on index {}", idx)
      }
    }
    case Replicas(prop, accep, learn) => {
      proposers = prop
      acceptors = accep
      learners = learn
    }
    case Election(idx) => {
      val runner = context.actorOf(Props(new ElectionRunner(idx)))
      runner ! StartElection(sender)
    }
    case DecidedLeader(idx, leader) => leaders.put(idx, leader)
    case Ping => sender ! Pong
    case Stop => context.stop(self)

    // used to verify leaders before the evaluation starts
    case VerifyLeaders => {
      sender ! Leaders(leaders)
    }
  }
}