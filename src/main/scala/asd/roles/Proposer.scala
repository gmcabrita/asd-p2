package asd.roles

import akka.actor.{Actor, ActorRef, Props}

class Proposer(acceptors: List[ActorRef], quorum: Int) extends Actor {
  var highest_n: Int = 0

  class ProposeSender(s: ActorRef, reply_to: ActorRef) extends Actor {
    def receive = {
      case Start => s ! Prepare(highest_n)
      case PrepareOk(na, va) => reply_to ! PrepareOk(na, va)
    }
  }

  class AcceptSender(s: ActorRef, reply_to: ActorRef, va: Pair) extends Actor {
    def receive = {
      case Start => s ! Accept(highest_n, va)
      case AcceptOk(n) => reply_to ! AcceptOk(n)
    }
  }

  class waiting_for_accept_ok(respond_to: ActorRef, received: Int, va: Pair) extends Actor {
    def receive = {
      case AcceptOk(n) => {
        // TODO: cenas
      }
    }
  }

  class waiting_for_propose_ok(respond_to: ActorRef, received: Int, v: Pair, na: Int, va: Pair) extends Actor {
    def receive = {
      case PrepareOk(na_ok, va_ok) => {
        if (na_ok > na) {
          context.become(waiting_for_propose_ok(respond_to, received + 1, v, na_ok, va_ok))
        } else {
          context.become(waiting_for_propose_ok(respond_to, received + 1, v, na, va))
        }

        if (received + 1 == quorum) {
          if (na > 0) highest_n = na

          val children = acceptors.map(s => context.actorOf(Props(new AcceptSender(s, self, va))))
          children.foreach(_ ! Start)
          context.setReceiveTimeout(timeout.duration)
          context.become(waiting_for_accept_ok(respond_to, 0, va))
        }
      }
    }
  }

  def receive = {
    case Put(key, value) => {
      // enviar Prepare para Acceptor
      highest_n += 1

      val children = acceptors.map(s => context.actorOf(Props(new ProposeSender(s, self))))
      children.foreach(_ ! Start)
      context.setReceiveTimeout(timeout.duration)
      context.become(waiting_for_propose_ok(sender, 0, Pair(key, value), 0, null))
    }
    case Get(key) => {
      // enviar Prepare para Acceptor
    }
    case PrepareOk => {
      // enviar Accept para Acceptor
    }
    case AcceptOk => {
      // enviar Decided para todos os Acceptors
    }
  }
}