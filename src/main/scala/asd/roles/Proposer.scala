package asd.roles

import akka.actor.{Actor, ActorRef, Props}

class Proposer(acceptors: List[ActorRef], quorum: Int) extends Actor {
  // store for the highest n of each key
  var n_store = new ParHashMap[String, Int]

  class waiting_for_accept_ok(respond_to: ActorRef, received: Int, va: Pair) extends Actor {
    def receive = {
      case AcceptOk(n) => {
        // TODO:
        // if (na_ok > na) {
        //   context.become(waiting_for_propose_ok(respond_to, received + 1, v, na_ok, va_ok))
        // } else {
        //   context.become(waiting_for_propose_ok(respond_to, received + 1, v, na, va))
        // }

        // if (received + 1 == quorum) {
        //   if (na > 0) highest_n = na

        //   val children = acceptors.map(s => context.actorOf(Props(new AcceptSender(s, self, va))))
        //   children.foreach(_ ! Start)
        //   context.setReceiveTimeout(timeout.duration)
        //   context.become(waiting_for_accept_ok(respond_to, 0, va))
        // }
      }
      case ReceiveTimeout => {
        // log.warning("Timeout on replication. Was: {}, Received: {}, Result: {}", self, received, result)
        // respond_to ! Timedout
        // context.become(receive)
        // TODO:
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

          val children = acceptors.par.each(s => s ! Accept(highest_n, va))
          children.foreach(_ ! Start)
          context.setReceiveTimeout(timeout.duration)
          context.become(waiting_for_accept_ok(respond_to, 0, va))
        }
      }
      case ReceiveTimeout => {
        // log.warning("Timeout on replication. Was: {}, Received: {}, Result: {}", self, received, result)
        // respond_to ! Timedout
        // context.become(receive)
        // TODO:
      }
    }
  }

  def receive = {
    case Put(key, value) => {
      // TODO:
      // are we the leader?
      // no -> send operation to leader (leader needs the client ActorRef!)
      // yes -> keep going
      //   retrieve the servers that should be replicated with this key
      //   retrieve the highest_n belonging to this key and increase it by 1
      //   send message to all the relevant acceptors and change context

      // val children = acceptors.par.each(s => s ! Prepare(key, highest_n))
      // children.foreach(_ ! Start)
      // context.setReceiveTimeout(timeout.duration)
      // context.become(waiting_for_propose_ok(sender, 0, Pair(key, value), 0, null))
    }
    case Get(key) => {
      // are we the leader?
      // no -> send operation to leader (leader needs the client ActorRef!)
      // yes -> keep going
      //   retrieve the servers that should be replicated with this key
      //   retrieve the highest_n belonging to this key and increase it by 1
      //   send message to all the relevant acceptors and change context

      // TODO: can we make this operation faster?
      // maybe we don't need to run paxos for this one since we're just reading.
    }
    case PrepareOk => {
      // enviar Accept para Acceptor
    }
    case AcceptOk => {
      // enviar Decided para todos os Acceptors
    }
  }
}