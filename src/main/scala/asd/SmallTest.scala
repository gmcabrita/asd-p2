package asd

import asd.clients.Client
import asd.roles.{Proposer, Acceptor, Learner}
import asd.evaluation._
import asd.messages.{Put, Get}

import akka.actor.{ActorRef, Props, ActorSystem}

object SmallTest extends App {
  implicit val system = ActorSystem("MAIN")

  val num_servers = 12
  val num_clients = 3

  val num_replicas = 3
  val quorum = 2

  val learners: Vector[ActorRef] = (1 to num_servers).toVector.map(_ => system.actorOf(Props[Learner]))
  val acceptors: Vector[ActorRef] = (1 to num_servers).toVector.map(i => system.actorOf(Props(new Acceptor(learners.toList, i - 1))))
  val proposers: Vector[ActorRef] = (1 to num_servers).toVector.map(i => system.actorOf(Props(new Proposer(acceptors.toList, learners.toList, num_replicas, quorum, i - 1))))
  val clients: Vector[ActorRef] = (1 to num_clients).toVector.map(_ => system.actorOf(Props(new Client(proposers.toList, num_replicas, quorum))))

  clients(0) ! Put("x", "test")
  clients(1) ! Put("x", "hello")
  // clients(2) ! Get("x")
  // Thread.sleep(250)
  // clients(2) ! Get("x")
  // Thread.sleep(250)
  // clients(2) ! Get("x")
}