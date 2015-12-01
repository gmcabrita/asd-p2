package asd

import asd.messages.WarmUp
import asd.evaluation._
import akka.actor.{ActorSystem, Props}

object KVStore extends App {
  implicit val system = ActorSystem("MAIN")

  val eval = system.actorOf(Props(new LocalEvaluation(
    1000, // num keys
    12, // num servers
    512, // num clients
    3, // num replicas
    2, // quorum
    10000, // run time in milliseconds
    (50, 50), // rw ratio
    192371441 // seed
  )))

  eval ! WarmUp
}