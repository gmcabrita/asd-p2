package asd

import asd._
import asd.messages._
import akka.actor.{ActorRef, Props, ActorSystem}
import com.typesafe.config.ConfigFactory

import java.io.File

object RemoteSystemOne extends App {
  val config = ConfigFactory.load("baseActorSystem")
  val system = ActorSystem("remoteSystem1", config.getConfig("remote1").withFallback(config))
}