package asd.evaluation

import asd.evaluation._
import asd.messages._
import asd.rand.Zipf
import asd.roles.{Acceptor, Learner, Proposer}
import asd.clients.Client

import akka.actor.{Actor, ActorRef, Props, ActorSystem, ReceiveTimeout, Deploy, AddressFromURIString}
import akka.remote.RemoteScope
import akka.event.{Logging, LoggingAdapter}
import akka.event.Logging.LogLevel._
import akka.pattern.ask
import akka.util.Timeout

import com.typesafe.config.ConfigFactory

import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.parallel.mutable.ParHashMap
import scala.util.{Success, Failure, Random}

import java.io.File

class DistributedEvaluation(num_keys: Int, num_servers: Int, num_clients: Int, num_replicas: Int, quorum: Int, run_time: Long, rw_ratio: (Int, Int), seed: Int, system: ActorSystem) extends Actor {
  implicit val timeout = Timeout(3000 milliseconds)

  val zipf = new Zipf(num_keys, seed)
  val r = new Random(seed)

  val log = Logging.getLogger(context.system, this)
  val config = ConfigFactory.parseFile(new File("src/main/resources/deploy.conf")).resolve()

  val s1 = AddressFromURIString(config.getString("remote1.path"))
  val s2 = AddressFromURIString(config.getString("remote2.path"))

  val faults = List() //List(0, 3, 6, 9)
  val num_faults = faults.length

  val learners: Vector[ActorRef] = (1 to num_servers).toVector.map(i => {
    if (i <= num_servers/2) {
      system.actorOf(Props(classOf[Learner]).withDeploy(Deploy(scope = RemoteScope(s1))), "l1"+i)
    } else {
      system.actorOf(Props(classOf[Learner]).withDeploy(Deploy(scope = RemoteScope(s2))), "l2"+i)
    }
  })
  val acceptors: Vector[ActorRef] = (1 to num_servers).toVector.map(i => {
    if (i <= num_servers/2) {
      system.actorOf(Props(classOf[Acceptor], learners.toList, i - 1).withDeploy(Deploy(scope = RemoteScope(s1))), "a1"+i)
    } else {
      system.actorOf(Props(classOf[Acceptor], learners.toList, i - 1).withDeploy(Deploy(scope = RemoteScope(s2))), "a2"+i)
    }
  })
  val proposers: Vector[ActorRef] = (1 to num_servers).toVector.map(i => {
    if (i <= num_servers/2) {
      system.actorOf(Props(classOf[Proposer], learners, num_replicas, num_faults, quorum, i - 1).withDeploy(Deploy(scope = RemoteScope(s1))), "p1"+i)
    } else {
      system.actorOf(Props(classOf[Proposer], learners, num_replicas, num_faults, quorum, i - 1).withDeploy(Deploy(scope = RemoteScope(s2))), "p2"+i)
    }
  })

  val proposer_replicas: Vector[Vector[ActorRef]] = proposers.sliding(num_replicas).toVector ++ (proposers.takeRight(num_replicas - 1) ++ proposers.take(num_replicas - 1)).sliding(num_replicas).toVector

  val acceptors_replicas: Vector[Vector[ActorRef]] = acceptors.sliding(num_replicas).toVector ++ (acceptors.takeRight(num_replicas - 1) ++ acceptors.take(num_replicas - 1)).sliding(num_replicas).toVector

  val learners_replicas: Vector[Vector[ActorRef]] = learners.sliding(num_replicas).toVector ++ (learners.takeRight(num_replicas - 1) ++ learners.take(num_replicas - 1)).sliding(num_replicas).toVector

  val clients: Vector[ActorRef] = (1 to num_clients).toVector.map(i => {
    if (i <= num_clients/2 || num_clients == 1) {
      system.actorOf(Props(classOf[Client], proposer_replicas, num_replicas, quorum).withDeploy(Deploy(scope = RemoteScope(s1))), "c1"+i)
    } else {
      system.actorOf(Props(classOf[Client], proposer_replicas, num_replicas, quorum).withDeploy(Deploy(scope = RemoteScope(s2))), "c2"+i)
    }
  })

  var leaders = new ParHashMap[Int, ActorRef]

  var reads: Long = 0
  var writes: Long = 0

  var begin: Long = 0
  var end: Long = 0

  // fault injection
  faults.foreach(i => {
    learners(i) ! Stop
    acceptors(i) ! Stop
    proposers(i) ! Stop
  })

  def continue(client: ActorRef) = {
    val time = System.nanoTime

    if (time - begin >= run_time * 1e6) {
      end = time

      Thread.sleep(2000)

      val results: Vector[Future[Any]] = clients.map(c => ask(c, Stop))
      val final_results = Future.fold[Any, (Double, Double, Double)](results)((0, Double.MinValue, Double.MaxValue))((acc, r) => {
        (acc, r) match {
          case ((avg, high, low), AvgLatency(v)) => {
            if (v > high && v < low) (avg + v, v, v)
            else if (v > high) (avg + v, v, low)
            else if (v < low) (avg + v, high, v)
            else (avg + v, high, low)
          }
        }
      })

      val operations = reads + writes
      val t = (end - begin)/1e6
      println("reads: " + reads)
      println("writes: " + writes)
      println("operations: " + operations)
      println("elapsed time: " + t + "ms")
      println("throughput: " + ((operations*10000) / t))

      Await.result(final_results, 1 second).asInstanceOf[(Double, Double, Double)] match {
        case (avg, high, low) => {
          println("Average client latency: " + avg / results.length)
          println("Highest client latency: " + high)
          println("Lowest client latency: " + low)
        }
      }

      Thread.sleep(2000)

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
    case WarmUp => {
      proposers.par.foreach(_ ! Replicas(proposer_replicas, acceptors_replicas, learners_replicas))

      proposers.par.foreach(p => {
        (0 to (num_servers - 1)).toList.foreach(i => p ! Election(i))
      })
      context.become(waiting_for_election(0, 0))
    }
  }

  def running(): Receive = {
    case Start => {
      begin = System.nanoTime
      clients.foreach(_ ! gen_op())
    }
    case _ => continue(sender)
  }

  def waiting_for_election(received_acks: Int, received_leaders: Int): Receive = {
    case Ack => {
      if (received_acks + 1 == num_servers) {
        Thread.sleep(4000)
        proposers.foreach(_ ! VerifyLeaders) // force to print leaders
        log.info("System has finished electing all possible leaders.")
        log.info("Verifying leaders on each node...")
        context.become(waiting_for_election(received_acks + 1, received_leaders))
      } else {
        context.become(waiting_for_election(received_acks + 1, received_leaders))
      }
    }
    case Leaders(l) => {
      if (leaders.isEmpty) {
        leaders = l
      } else {
        if (!l.equals(leaders)) {
          log.warning("Two servers have different leaders on at least one set of replicas.")
          log.warning("Right: {}", leaders)
          log.warning("Left: {}", l)
        }
      }

      if (received_leaders + 1 == num_servers - num_faults) {
        log.info("All servers have the same leaders.")
        self ! Start
        context.become(running)
      } else {
        context.become(waiting_for_election(received_acks, received_leaders + 1))
      }
    }
    case ReceiveTimeout => {
      if (received_acks < num_servers) log.warning("Timeout on election.")
      if (received_leaders < num_servers) log.warning("Timeout on leader verification.")
    }
  }
}