package asd.evaluation

import asd.messages._
import asd.rand.Zipf
import asd.roles.{Acceptor, Learner, Proposer}
import asd.clients.Client

import akka.actor.{Actor, ActorRef, ActorSystem, Props}

import scala.util.Random

class LocalEvaluation(num_keys: Int, num_servers: Int, num_clients: Int, num_replicas: Int, quorum: Int, run_time: Long, rw_ratio: (Int, Int), seed: Int) extends Actor {
  val zipf = new Zipf(num_keys, seed)
  val r = new Random(seed)

  implicit val system = ActorSystem("EVAL")

  val learners: Vector[ActorRef] = (1 to num_servers).toVector.map(_ => system.actorOf(Props[Learner]))
  val acceptors: Vector[ActorRef] = (1 to num_servers).toVector.map(_ => system.actorOf(Props(new Acceptor(learners.toList, num_replicas))))
  val proposers: Vector[ActorRef] = (1 to num_servers).toVector.map(i => system.actorOf(Props(new Proposer(acceptors.toList, learners.toList, num_replicas, quorum, i - 1))))
  val clients: Vector[ActorRef] = (1 to num_clients).toVector.map(_ => system.actorOf(Props(new Client(proposers.toList, num_replicas, quorum))))

  var operations = 0
  var reads: Long = 0
  var writes: Long = 0

  var begin: Long = 0
  var end: Long = 0

  def continue(client: ActorRef) = {
    operations += 1

    val time = System.nanoTime
    if (time - begin >= run_time) {
      end = time
      println("reads: " + reads)
      println("writes: " + writes)
      println("elapsed time: " + (end - begin)/1e6+"ms")

      clients.par.foreach(_ ! Stop)
      sys.exit(0)
    }

    client ! gen_op()
  }

  def gen_op() = {
    val float = r.nextFloat()
    val key = zipf.nextZipf().toString
    if (float > (rw_ratio._2 / 100f)) { // read
      reads += 1
      Get(key)
    } else { // write
      val value = r.nextString(16)
      writes += 1
      Put(key, value)
    }
  }

  def receive = {
    case Start => {
      begin = System.nanoTime
      operations += num_clients
      clients.foreach(_ ! gen_op())
    }
    case _ => continue(sender)
  }
}