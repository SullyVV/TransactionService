package rings

import akka.actor.{ActorSystem, ActorRef, Props}

sealed trait AppServiceAPI
case class Prime() extends AppServiceAPI
case class Command() extends AppServiceAPI
case class View(endpoints: Seq[ActorRef]) extends AppServiceAPI
case class GetLock(clientID: Int, opsArray: scala.collection.mutable.ArrayBuffer[Operation]) extends AppServiceAPI
case class UnLock(clientID: Int, key: BigInt) extends AppServiceAPI
case class Commit(clientID: Int, writeArray: scala.collection.mutable.ArrayBuffer[WriteElement]) extends AppServiceAPI
case class DirtyData(key: BigInt) extends AppServiceAPI
case class CommitDecision(clientID: Int, decision: Boolean) extends AppServiceAPI
case class Put(key: BigInt, value: Int) extends AppServiceAPI
case class Get(key: BigInt) extends AppServiceAPI
case class TransactionBegin() extends AppServiceAPI
case class TransactionRead(key: BigInt) extends AppServiceAPI
case class TransactionWrite(key: BigInt) extends AppServiceAPI
case class TransactionCommit() extends AppServiceAPI

/**
 * This object instantiates the service tiers and a load-generating master, and
 * links all the actors together by passing around ActorRef references.
 *
 * The service to instantiate is bolted into the KVAppService code.  Modify this
 * object if you want to instantiate a different service.
 */


object KVAppService {

  def apply(system: ActorSystem, numClient: Int, numServer: Int): ActorRef = {

    /** Storage tier: create K/V store servers */
    val stores = for (i <- 0 until numServer)
      yield system.actorOf(KVStore.props(), "RingStore" + i)

    /** Service tier: create app servers */
    val servers = for (i <- 0 until numClient)
      yield system.actorOf(RingServer.props(i, numClient, stores,1), "RingServer" + i)

    for (store <- stores) {
      store ! View(servers)
    }

    /** If you want to initialize a different service instead, that previous line might look like this:
      * yield system.actorOf(GroupServer.props(i, numNodes, stores, ackEach), "GroupServer" + i)
      * For that you need to implement the GroupServer object and the companion actor class.
      * Following the "rings" example.
      */

    /** Load-generating master */
    val master = system.actorOf(LoadMaster.props(numClient, servers, 1), "LoadMaster")
    master
  }
}

