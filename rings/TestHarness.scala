package rings

import scala.concurrent.duration._
import scala.concurrent.Await

import akka.actor.{Actor, ActorSystem, ActorRef, Props}
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout

object TestHarness {
  val system = ActorSystem("Rings")
  implicit val timeout = Timeout(1000 seconds)
  val numClient = 10
  val numServer = 2
  // Service tier: create app servers and a Seq of per-node Stats
  val master = KVAppService(system, numClient, numServer)

  def main(args: Array[String]): Unit = run()

  def run(): Unit = {
    val future = ask(master, Start())
    Await.result(future, timeout.duration).asInstanceOf[Boolean]
    system.shutdown()
//    val s = System.currentTimeMillis
//    runUntilDone
//    val runtime = System.currentTimeMillis - s
//    //val throughput = (opsPerNode * numNodes)/runtime
//
//    //println(s"Done in $runtime ms ($throughput Kops/sec)")

  }

  def runUntilDone() = {
    master ! Start()
    val future = ask(master, Join()).mapTo[Stats]
    val done = Await.result(future, 60 seconds)
  }



}
