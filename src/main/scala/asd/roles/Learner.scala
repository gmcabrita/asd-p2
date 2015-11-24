package asd.roles

import akka.actor.{Actor, ActorRef}
import scala.collection.parallel.mutable.ParHashMap

class Learner extends Actor {
  // use a "state machine" for every key
  var store = new ParHashMap[String, List[String]]

  def receive = {
    case Result => {
      // responder ao cliente
    }
  }
}