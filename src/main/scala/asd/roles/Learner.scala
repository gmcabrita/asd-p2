package asd.roles

import akka.actor.{Actor}

class Learner extends Actor {
  def receive = {
    case Result => {
      // responder ao cliente
    }
  }
}