package asd

import asd.clients.Client
import asd.roles.{Proposer, Acceptor, Learner}
import asd.evaluation._
import asd.messages.{Start, Put, Get, Stop}

import akka.actor.{Actor, ActorRef, Props, ActorSystem}

object SmallTest extends App {
  class Foo(operations: List[Any]) extends Actor {
    val num_servers = 12
    val num_clients = 3

    val num_replicas = 3
    val quorum = 2

    var op = 0

    val learners: Vector[ActorRef] = (1 to num_servers).toVector.map(_ => system.actorOf(Props[Learner]))
    val acceptors: Vector[ActorRef] = (1 to num_servers).toVector.map(i => system.actorOf(Props(new Acceptor(learners.toList, i - 1))))
    val proposers: Vector[ActorRef] = (1 to num_servers).toVector.map(i => system.actorOf(Props(new Proposer(acceptors.toList, learners.toList, num_replicas, quorum, i - 1))))
    val clients: Vector[ActorRef] = (1 to num_clients).toVector.map(_ => system.actorOf(Props(new Client(proposers.toList, num_replicas, quorum))))

    def receive = {
      case Start => {
        clients(0) ! operations(0)
        clients(1) ! operations(1)
        op += 2
      }
      case Stop => {
        sys.exit(0)
      }
      case _ => {
        if (op < operations.length) {
          sender ! operations(op)
          op += 1
        } else {
          self ! Stop
          clients(0) ! Stop
          clients(1) ! Stop
        }
      }
    }
  }
  implicit val system = ActorSystem("MAIN")

  val ops = List(Put("x", "goodbye"), Put("x", "hello"), Get("x"))
  val foo = system.actorOf(Props(new Foo(ops)))

  foo ! Start
}