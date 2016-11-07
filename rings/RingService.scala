package rings

import javax.sql.rowset.spi.TransactionalWriter

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.event.Logging

class RingCell(var prev: BigInt, var next: BigInt)
class RingMap extends scala.collection.mutable.HashMap[BigInt, RingCell]

/**
 * RingService is an example app service for the actor-based KVStore/KVClient.
 * This one stores RingCell objects in the KVStore.  Each app server allocates new
 * RingCells (allocCell), writes them, and reads them randomly with consistency
 * checking (touchCell).  The allocCell and touchCell commands use direct reads
 * and writes to bypass the client cache.  Keeps a running set of Stats for each burst.
 *
 * @param myNodeID sequence number of this actor/server in the app tier
 * @param numNodes total number of servers in the app tier
 * @param storeServers the ActorRefs of the KVStore servers
 * @param burstSize number of commands per burst
 */

class RingServer (val myNodeID: Int, val numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int, system: ActorSystem) extends Actor {
  val generator = new scala.util.Random
  val kvclient = new KVClient(myNodeID, storeServers, system)
  val dirtycells = new IntMap
  val localWeight: Int = 70
  val log = Logging(context.system, this)
  val isAlive = true
  var stats = new Stats
  var allocated: Int = 0
  var endpoints: Option[Seq[ActorRef]] = None

  def receive() = {
      case TransactionBegin() =>
        tBegin()
      case TransactionRead(key) =>
        tRead(key)
      case TransactionWrite(key) =>
        tWrite(key)
      case TransactionCommit() =>
        tCommit()
//      case Prime() =>
//        allocCell
//      //     rwcheck(myNodeID, new RingCell(0,0))
//      case Command() =>
//        incoming(sender)
//        command
      case View(e) =>
        endpoints = Some(e)
      case DirtyData(key) =>
        cleanCache(key)
        sender ! true
      case AliveCheck(key) =>
        sender ! isAlive
      case DeadClient() =>
        cleanClient()
  }

  private def cleanClient() = {
    kvclient.clearClient()
  }

  private def cleanCache(key: BigInt) = {
    kvclient.clearClient()
  }

  private def tBegin() = {
    kvclient.begin()
  }

  private def tRead(key: BigInt) = {
    kvclient.transactionRead(key)
  }

  private def tWrite(key: BigInt) = {
    kvclient.transactionWrite(key)
  }

  private def tCommit() = {
    kvclient.transactionCommit()
  }
}

object RingServer {
  def props(myNodeID: Int, numNodes: Int, storeServers: Seq[ActorRef], burstSize: Int, system: ActorSystem): Props = {
    Props(classOf[RingServer], myNodeID, numNodes, storeServers, burstSize, system)
  }
}
