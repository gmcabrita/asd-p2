package asd.message

case class Pair(key: String, value: String)

case class Start()
case class Prepare(n: Int)
case class PrepareOk(na: Int, va: Pair)
case class Accept(n: Int, va: Pair)
case class AcceptOk(n: Int)
case class Decided(va: Pair)
case class Put(key: String, value: String)
case class Get(key: String)
case class Result(va: Pair)