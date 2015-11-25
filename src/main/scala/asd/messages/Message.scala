package asd.messages

case class Start()
case class Stop()

case class Prepare(key: String, n: Int)
case class PrepareOk(key: String, na: Int, va: String)
case class PrepareTooLow(key: String, n: Int)
case class Accept(key: String, n: Int, va: String)
case class AcceptOk(key: String, n: Int)
case class Decided(key: String, value: String)

case class PrepareLeader(n: Int)
case class PrepareLeaderOk(na: Int, va: Int)
case class AcceptLeader(n: Int, va: Int)
case class AcceptLeaderOk(n: Int)
case class DecidedLeader(va: Int)

case class Put(key: String, value: String)
case class Get(key: String)
case class Ack()
case class Result(key: String, v: Option[String])