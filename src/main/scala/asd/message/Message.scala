package asd.message

import akka.actor.ActorRef

case class Start()
case class ReceiveTimeout()

case class Prepare(key: String, n: Int)
case class PrepareOk(key: String, na: Int, va: String)
case class Accept(key: String, n: Int, va: String)
case class AcceptOk(key: String, n: Int)
case class Decided(key: String, va: String)

case class PrepareLeader(n: Int)
case class PrepareLeaderOk(na: Int, va: Int)
case class AcceptLeader(n: Int, va: Int)
case class AcceptLeaderOk(n: Int)
case class DecidedLeader(va: Int)

case class Put(key: String, value: String)
case class Get(key: String)
case class Result(va: Pair)
case class ElectLeader(leader: ActorRef)
case class Leader()

case class Ping()
case class Pong()
case class Timedout()