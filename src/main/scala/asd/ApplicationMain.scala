package asd

import asd.evaluation._
import akka.actor.ActorSystem

object KVStore extends App {
  implicit val system = ActorSystem("MAIN")

  // val eval = system.actorOf(Props(new LocalEvaluation(
  //   1000, // num keys
  //   12, // num clients
  //   12, // num servers
  //   7, // quorum
  //   12, // degree of replication
  //   192371441, // seed
  //   true, // linearizable?
  //   10000, // number of operations
  //   5, // number of injected faults
  //   1 // runs per case
  // )))

  // eval ! Start
}