package asd.messages

import akka.actor.ActorRef
import scala.collection.parallel.mutable.ParHashMap

case class Start()
case class WarmUp()
case class Stop()

case class Decided(key: String, value: String)

case class Replicas(proposers: Vector[Vector[ActorRef]], acceptors: Vector[Vector[ActorRef]], learners: Vector[Vector[ActorRef]])

case class PrepareLeader(idx: Int, n: Int)
case class PrepareLeaderOk(idx: Int, na: Int, va: ActorRef)
case class PrepareLeaderTooLow(idx: Int, n: Int)
case class AcceptLeader(idx: Int, n: Int, va: ActorRef)
case class AcceptLeaderOk(idx: Int, n: Int)
case class DecidedLeader(idx: Int, va: ActorRef)

case class Put(key: String, value: String)
case class Get(key: String)
case class CPut(client: ActorRef, key: String, value: String)
case class CGet(client: ActorRef, key: String)
case class Ack()
case class Stored()
case class Result(key: String, v: Option[String])

case class Ping()
case class Pong()
case class Continue()
case class Election(idx: Int)
case class StartElection(sender: ActorRef)

case class VerifyLeaders()
case class Leaders(leaders: ParHashMap[Int, ActorRef])

case class AvgLatency(avg: Double)