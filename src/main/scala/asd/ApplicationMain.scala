package asd

import asd.messages.WarmUp
import asd.evaluation._
import akka.actor.{ActorSystem, Props, AddressFromURIString, Deploy}
import akka.remote.RemoteScope

import com.typesafe.config.ConfigFactory
import java.io.File

object KVStore extends App {
  val config = ConfigFactory.parseFile(new File("src/main/resources/deploy.conf")).resolve()
  implicit val system = ActorSystem("DeployerSystem", config)

  val d = AddressFromURIString(config.getString("deployer.path"))

  val eval = system.actorOf(Props(new DistributedEvaluation(
    1000, // num keys
    12, // num servers
    12, // num clients
    3, // num replicas
    2, // quorum
    10000, // run time in milliseconds
    (50, 50), // rw ratio
    192371441, // seed
    system
  )).withDeploy(Deploy(scope = RemoteScope(d))), "deployer")

  // val eval = system.actorOf(Props(new LocalEvaluation(
  //   1000, // num keys
  //   12, // num servers
  //   12, // num clients
  //   3, // num replicas
  //   2, // quorum
  //   10000, // run time in milliseconds
  //   (50, 50), // rw ratio
  //   192371441 // seed
  // )))

  eval ! WarmUp
}