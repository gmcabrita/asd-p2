package asd.roles

import asd.messages._

import akka.actor.{Actor, ActorRef}

import scala.collection.parallel.mutable.ParHashMap

class Learner extends Actor {

  var store = new ParHashMap[String, String]

  def receive = {
    case Decided(key, value) => {
      store.put(key, value)
    }
    case Get(key) => {
      val value = store.get(key) match {
        case Some(v) => Result(key, Some(v))
        case None => Result(key, None)
      }
      sender ! value
    }
  }
}