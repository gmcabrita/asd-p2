package asd

import asd.clients.Client
import asd.roles.{Proposer, Acceptor, Learner}
import asd.evaluation._
import asd.messages.{WarmUp, Start, Stop, Election, VerifyLeaders, Leaders, Put, Get, Replicas, Ack, DecidedLeader, AvgLatency}

import akka.actor.{Actor, ActorRef, Props, ActorSystem, ReceiveTimeout}
import akka.event.{Logging, LoggingAdapter}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.parallel.mutable.ParHashMap
import scala.util.{Success, Failure}

object SmallTest extends App {

  implicit val timeout = Timeout(6000 milliseconds)

  class Foo(operations: List[Any]) extends Actor {
    val log = Logging.getLogger(context.system, this)

    val num_servers = 12
    val num_clients = 3

    val num_replicas = 3
    val quorum = 2

    var op = 0

    val learners: Vector[ActorRef] = (1 to num_servers).toVector.map(_ => system.actorOf(Props[Learner]))
    val acceptors: Vector[ActorRef] = (1 to num_servers).toVector.map(i => system.actorOf(Props(new Acceptor(learners.toList, i - 1))))
    val proposers: Vector[ActorRef] = (1 to num_servers).toVector.map(i => system.actorOf(Props(new Proposer(learners, num_replicas, quorum, i - 1))))

    val proposer_replicas: Vector[Vector[ActorRef]] = proposers.sliding(num_replicas).toVector ++ (proposers.takeRight(num_replicas - 1) ++ proposers.take(num_replicas - 1)).sliding(num_replicas).toVector

    val acceptors_replicas: Vector[Vector[ActorRef]] = acceptors.sliding(num_replicas).toVector ++ (acceptors.takeRight(num_replicas - 1) ++ acceptors.take(num_replicas - 1)).sliding(num_replicas).toVector

    val learners_replicas: Vector[Vector[ActorRef]] = learners.sliding(num_replicas).toVector ++ (learners.takeRight(num_replicas - 1) ++ learners.take(num_replicas - 1)).sliding(num_replicas).toVector

    proposers.par.foreach(_ ! Replicas(proposer_replicas, acceptors_replicas, learners_replicas))

    val clients: Vector[ActorRef] = (1 to num_clients).toVector.map(_ => system.actorOf(Props(new Client(proposer_replicas, num_replicas, quorum))))

    var leaders = new ParHashMap[Int, ActorRef]

    def running(): Receive = {
      case Start => {
        clients(0) ! operations(0)
        clients(1) ! operations(1)
        op += 2
      }
      case _ => {
        if (op < operations.length) {
          sender ! operations(op)
          op += 1
        } else {
          Thread.sleep(10000)

          val results: Vector[Future[Any]] = clients.map(c => ask(c, Stop)) //.mapTo(AvgLatency)
          results.par.foreach(f => f onComplete {
            case Success(v) => println("Average latency: " + v)
            case Failure(v) => println("Error on future: " + v)
          })

          Thread.sleep(10000)

          sys.exit(0)
        }
      }
    }

    def waiting_for_election(received_acks: Int, received_leaders: Int): Receive = {
      case Ack => {
        if (received_acks + 1 == num_servers) {
          Thread.sleep(4000)
          proposers.foreach(_ ! VerifyLeaders) // force to print leaders
          log.info("System has finished electing all possible leaders.")
          log.info("Verifying leaders on each node...")
          context.become(waiting_for_election(received_acks + 1, received_leaders))
        } else {
          context.become(waiting_for_election(received_acks + 1, received_leaders))
        }
      }
      case Leaders(l) => {
        if (leaders.isEmpty) {
          leaders = l
        } else {
          if (!l.equals(leaders)) {
            log.warning("Two servers have different leaders on at least one set of replicas.")
            log.warning("Right: {}", leaders)
            log.warning("Left: {}", l)
          }
        }

        if (received_leaders + 1 == num_servers) {
          log.info("All servers have the same leaders.")
          self ! Start
          context.become(running)
        } else {
          context.become(waiting_for_election(received_acks, received_leaders + 1))
        }
      }
      case ReceiveTimeout => {
        if (received_acks < num_servers) log.warning("Timeout on election.")
        if (received_leaders < num_servers) log.warning("Timeout on leader verification.")
      }
    }

    def receive = {
      case WarmUp => {
        proposers.par.foreach(p => {
          (0 to (num_servers - 1)).toList.foreach(i => p ! Election(i))
        })
        context.become(waiting_for_election(0, 0))
      }
    }
  }
  implicit val system = ActorSystem("MAIN")

  val ops = List(Put("x", "goodbye"), Put("x", "hello"), Get("x"))
  val foo = system.actorOf(Props(new Foo(ops)))

  foo ! WarmUp
}