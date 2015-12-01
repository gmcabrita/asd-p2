package asd

import asd._
import asd.messages._
import akka.actor.{ActorRef, Props, ActorSystem}
import com.typesafe.config.ConfigFactory

import java.io.File

object RemoteSystemTwo extends App {
  val config = ConfigFactory.load("baseActorSystem")
  println(config.getConfig("remote2"))
  val system = ActorSystem("remoteSystem2", config.getConfig("remote2").withFallback(config))
}