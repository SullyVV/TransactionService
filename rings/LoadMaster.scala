package rings

import akka.actor.{Actor, ActorSystem, ActorRef, Props}
import akka.event.Logging

sealed trait LoadMasterAPI
case class Start() extends LoadMasterAPI
case class BurstAck(senderNodeID: Int, stats: Stats) extends LoadMasterAPI
case class Join() extends LoadMasterAPI

/** LoadMaster is a singleton actor that generates load for the app service tier, accepts acks from
  * the app tier for each command burst, and terminates the experiment when done.  It uses the incoming
  * acks to self-clock the flow of commands, to avoid swamping the mailbox queues in the app tier.
  * It also keeps running totals of various Stats returned by the app servers with each burst ack.
  * A listener may register via Join() to receive a message when the experiment is done.
  *
  * @param numNodes How many actors/servers in the app tier
  * @param servers ActorRefs for the actors/servers in the app tier
  * @param burstSize How many commands per burst
  */

class LoadMaster (val numNodes: Int, val servers: Seq[ActorRef], val burstSize: Int) extends Actor {
  val log = Logging(context.system, this)
  var active: Boolean = true
  var listener: Option[ActorRef] = None
  var nodesActive = numNodes
  var maxPerNode: Int = 0

  val serverStats = for (s <- servers) yield new Stats

  def receive = {
    case Start() =>
      log.info("Master starting bursts")
      transaction()
      Thread.sleep(500)
      sender ! true
//      maxPerNode = totalPerNode
//      for (s <- servers) {
//        s ! Prime()
//        burst(s)
//      }

    case BurstAck(senderNodeID: Int, stats: Stats) =>
      serverStats(senderNodeID) += stats
      val s = serverStats(senderNodeID)
      if (s.messages == maxPerNode) {
        println(s"node $senderNodeID done, $s")
        nodesActive -= 1
        if (nodesActive == 0)
          deactivate()
      } else {
        if (active)
          burst(servers(senderNodeID))
      }

    case Join() =>
      listener = Some(sender)
  }
  def transaction() = {
    /*************  deadlock check *************/
//    servers(0) ! TransactionBegin()
//    servers(1) ! TransactionBegin()
//    servers(0) ! TransactionWrite(0)
//    servers(1) ! TransactionRead(1)
//    //Thread.sleep(100)
//    servers(0) ! TransactionRead(1)
//    servers(1) ! TransactionRead(0)
//    //Thread.sleep(100)
//    servers(0) ! TransactionCommit()
//    servers(1) ! TransactionCommit()
    /********************************************/

    /*************  update check *************/
//    for (i <- 0 until 3) {
//      servers(i) ! TransactionBegin()
//      servers(i) ! TransactionWrite(1)
//      servers(i) ! TransactionCommit()
//    }
//    servers(0) ! TransactionBegin()
//    servers(0) ! TransactionWrite(1)
//    servers(0) ! TransactionWrite(2)
//    servers(0) ! TransactionWrite(3)
//    servers(0) ! TransactionCommit()
//    Thread.sleep(500)
//    servers(1) ! TransactionBegin()
//    servers(1) ! TransactionWrite(2)
//    servers(1) ! TransactionWrite(3)
//    servers(1) ! TransactionWrite(1)
//    servers(1) ! TransactionCommit()

    /********************************************/
    /*************  lock release check *************/
//          servers(0) ! TransactionBegin()
//          servers(0) ! TransactionWrite(1)
//          //Thread.sleep(50)
//          servers(1) ! TransactionBegin()
//          servers(1) ! TransactionWrite(2)
//          servers(1) ! TransactionWrite(3)
//          servers(1) ! TransactionWrite(1)
//          servers(1) ! TransactionCommit()
//          //Thread.sleep(1)
//          servers(0) ! TransactionRead(2)
//          servers(0) ! TransactionCommit()

    /******* same client two transaction test*******/
              servers(0) ! TransactionBegin
              servers(0) ! TransactionWrite(1)
              servers(0) ! TransactionWrite(2)
              servers(0) ! TransactionWrite(3)
              servers(0) ! TransactionCommit()
              //Thread.sleep(500)
              servers(0) ! TransactionBegin()
              servers(0) ! TransactionWrite(1)
              servers(0) ! TransactionWrite(2)
              servers(0) ! TransactionWrite(3)
              servers(0) ! TransactionCommit()
    /********************************************/
    /********************************************/
//          servers(0) ! TransactionBegin
//          servers(0) ! TransactionRead(2)
//          servers(0) ! TransactionWrite(2)
//          servers(0) ! TransactionWrite(4)
//          servers(0) ! TransactionCommit()
//          //Thread.sleep(500)
//          servers(1) ! TransactionBegin()
//          servers(1) ! TransactionWrite(2)
//          servers(1) ! TransactionWrite(3)
//          servers(1) ! TransactionRead(1)
//          servers(1) ! TransactionCommit()
    /********************************************/


  }
  def burst(server: ActorRef): Unit = {
//    log.info(s"send a burst to node $target")
    for (i <- 1 to burstSize)
      server ! Command()
  }

  def deactivate() = {
    active = false
    val total = new Stats
    serverStats.foreach(total += _)
    println(s"$total")
    if (listener.isDefined)
      listener.get ! total
  }
}

object LoadMaster {
   def props(numNodes: Int, servers: Seq[ActorRef], burstSize: Int): Props = {
      Props(classOf[LoadMaster], numNodes, servers, burstSize)
   }
}

