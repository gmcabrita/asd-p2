package asd.roles

import akka.actor.{Actor, ActorRef}

class Acceptor(learners: List[ActorRef]) extends Actor {
  def receive = {
    case Prepare => {
      // enviar PrepareOk para Proposer
    }
    case Accept => {
      // enviar AcceptOk para Proposer
    }
    case Decided => {
      // enviar para todos os learners
    }
  }
}