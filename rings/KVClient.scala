package rings

import java.text.SimpleDateFormat
import java.util.Date

import scala.concurrent.duration._
import scala.concurrent.Await
import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global

class IntMap extends scala.collection.mutable.HashMap[BigInt, Int]
/**
  ops: 0 --> read, 1 --> write, 2 --> begin, 3 --> commit, 4 --> abort
  all write means +1 to the old data
  **/
class Operation(var oID: Int, var ops: Int, var key: BigInt)
class OpsResult(var oID: Int, var res:Int) // res: true->success, false->failure
class WriteElement(var key: BigInt, var value: Int)
/**
 * KVClient implements a client's interface to a KVStore, with an optional writeback cache.
 * Instantiate one KVClient for each actor that is a client of the KVStore.  The values placed
 * in the store are of type Any: it is up to the client app to cast to/from the app's value types.
 **/

class KVClient (clientID: Int, stores: Seq[ActorRef], system: ActorSystem) {
  private var cache = new IntMap
  private val snapshotCache = new IntMap
  implicit val timeout = Timeout(5 seconds)
  private val opsLog = new scala.collection.mutable.ArrayBuffer[Operation]
  private val locksHolder = new scala.collection.mutable.ArrayBuffer[BigInt]
  //private val locksHolder = new scala.collection.mutable.ArrayBuffer[BigInt]
  private val votesTable = new mutable.HashMap[ActorRef, Boolean]
  private var oID = 0
  private val dateFormat = new SimpleDateFormat ("mm:ss")
  private val commitTable = new mutable.HashMap[ActorRef, scala.collection.mutable.ArrayBuffer[WriteElement]]
  private val acquireTable = new mutable.HashMap[ActorRef, scala.collection.mutable.ArrayBuffer[Operation]]
  private val heartbeatTable = new mutable.ArrayBuffer[ActorRef]
  private var isPartitioned = false
  system.scheduler.schedule(0 milliseconds,5  milliseconds) {
      heartbeat()
  }
  // simulate disconnect using a scheduler, handle partition condition in commit
//  system.scheduler.scheduleOnce(5 milliseconds) {
//    if (clientID == 0) {
//      println(s"client ${clientID} is partitioned")
//      isPartitioned = true
//    }
//  }
  /** HeartBeat Function **/
  def heartbeat(): Unit = {
    for (storeServer <- heartbeatTable ) {
      storeServer ! HeartBeat(clientID)
    }
  }
  /** transaction begin */
  def begin() = {
    // create a snapshot when begin
    // record begin and commit in case client fails when doing ops in cache
    for ((k,v) <- cache) {
      snapshotCache.clear()
      snapshotCache.put(k, v)
    }
    opsLog += new Operation(oID, 2, -1)
    oID = oID + 1
  }

  /** transaction read */
  def transactionRead(key: BigInt) = {
    opsLog += new Operation(oID, 0, key)
    oID = oID + 1
  }

  /** transaction write */
  def transactionWrite(key: BigInt) = {
    opsLog += new Operation(oID, 1, key)
    oID = oID + 1
  }

  /** transaction abort */
  def transactionAbort() = {
    // do nothing but clear everything like nothing happens
    opsLog += new Operation(oID, 4, -1)
    oID = oID + 1
    cleanUp()
  }

  /** clean up **/
  def cleanUp() = {
    opsLog.clear()
    votesTable.clear()
    locksHolder.clear()
    commitTable.clear()
    acquireTable.clear()
    heartbeatTable.clear()
    oID = 0
  }

  /** transaction commit */
  def transactionCommit(): Boolean = {
      // acquire locks of all involved data, single fail->abort, 2PL
      opsLog += new Operation(oID, 3, -1)
      oID = oID + 1
      println(s"client$clientID commit")
      groupAcquires(opsLog)

      /** retry version of acquire locks **/
      while (!Lock(acquireTable)) {
        println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[31mFailed: client ${clientID} failed in acquire locks\033[0m")
        unLock(locksHolder)
        Thread.sleep(5) // rest for 5 ms between each request
      }
      // after acquire all needed locks, put all related store server's ActorRef into the heartbeat table
      for ((k, v) <- acquireTable) {
        heartbeatTable += k
      }
      println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[32mSuccess: client ${clientID} success in acquire locks\033[0m")

//      /*** simulate client0 fails after get all required locks ***/
//      if (clientID == 0) {
//        isPartitioned = true
//        println(s"client ${clientID} is partitioned after lock")
//        /***** ignore recovery at this time ***/
//        Thread.sleep(30)
//        isPartitioned = false
//        println(s"client ${clientID} recovers after partition")
//      }
//      /************************************************************/

      // do all operations in local cache first
      for (currentOperation <- opsLog) {
        if (currentOperation.ops == 0 || currentOperation.ops == 1) {
          var tmp = getCurrValue(currentOperation.key)
          if (currentOperation.ops == 1) {
            tmp = tmp + 1
          }
          cache.put(currentOperation.key, tmp)
        }
      }

      // after all operations in local cache, now we need to do 2PC for all write ops, get votes from all participants
      groupWrites(opsLog)
      for ((k, v) <- commitTable) {
        val future = ask(k, Commit(clientID, v))
        val done = Await.result(future, timeout.duration).asInstanceOf[AckMsg]
        if (done.par) {
          clearClient()
          multicastPartition()
          return false
        } else {
          votesTable.put(k, done.result)
        }
      }

      /*** simulate client0 fails after obtained votes from all store servers ***/
//    if (clientID == 0) {
//      isPartitioned = true
//      println(s"client ${clientID} is partitioned after getting votes")
//      /** *** ignore recovery at this time ***/
//      Thread.sleep(30)
//      isPartitioned = false
//      println(s"client ${clientID} recovers after partition")
//    }
      /************************************************************/

      // traverse all element in votesTable, if find any false, abort
      for ((k, v) <- votesTable) {
        if (!v) {
          // inform all participants to abort
          //notifyParticipants(clientID, votesTable, false) // we should piggy back the unlock info with the notification
          notifyParticipants(clientID, acquireTable, false)
          // recover to the snapshot
          cache = snapshotCache
          cleanUp()
          println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[35mCache info: client ${clientID} transaction failure: cache is: ${cache} \033[0m")
          println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[31mError: client: $clientID, participants ${k} votes for abort\033[0m")
          return false
        }
      }
      // should use lockTable when notify, for decision waiter, they can commit write the change and free the lock, for lock holder, they can free the lock
      //notifyParticipants(clientID, votesTable, true)
      notifyParticipants(clientID, acquireTable, true)
      cleanUp()
      println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[35mCache info: client ${clientID} transaction success: cache is: ${cache} \033[0m")
      return true
  }

  /** group acquires by store server**/
  def groupAcquires(opsLog: scala.collection.mutable.ArrayBuffer[Operation]) = {
    for (op <- opsLog) {
      if (op.ops == 0 || op.ops == 1) {
        val tmp = route(op.key)
        if (!acquireTable.contains(tmp)) {
          acquireTable.put(tmp, new mutable.ArrayBuffer[Operation])
        }
        acquireTable(tmp) += new Operation(op.oID, op.ops, op.key)
      }
    }
  }

  /** group Writes by store server**/
  def groupWrites(opsLog: scala.collection.mutable.ArrayBuffer[Operation]) = {
    for (op <- opsLog) {
      if (op.ops == 1) {
        // only need to handle write when collecting votes
        val tmp = route(op.key)
        if (!commitTable.contains(tmp)) {
          commitTable.put(tmp, new mutable.ArrayBuffer[WriteElement])
        }
        commitTable(tmp) += new WriteElement(op.key, cache(op.key))
      }
    }
  }

  /** get current value of the key **/
  def getCurrValue(key: BigInt) : Int = {
    var tmp: Int = 0
    if (cache.contains(key)) {
      // cache has data
      tmp = cache(key)
    } else {
      // cache doesnt have data, go get the data from server
      tmp = directRead(key)
      cache.put(key, tmp)
    }
    return tmp
  }

  /** Notify all participants about the result **/
//  def notifyParticipants(clientID: Int, voteTable: mutable.HashMap[ActorRef, Boolean], decision: Boolean) = {
//    for ((k,v) <- voteTable) {
//      k ! CommitDecision(clientID, decision)
//    }
//  }

  def notifyParticipants(clientID: Int, acquireTable: mutable.HashMap[ActorRef, scala.collection.mutable.ArrayBuffer[Operation]], decision: Boolean) = {
   for ((k, v) <- acquireTable) {
      k ! CommitDecision(clientID, decision)
    }
  }

  def multicastPartition() = {
    for ((k, v) <- acquireTable) {
      k ! PartitionedClient(clientID)
    }
  }

  /** Data lock **/
  def Lock(acquireTable: mutable.HashMap[ActorRef, scala.collection.mutable.ArrayBuffer[Operation]]): Boolean = {
    for ((k,v) <- acquireTable) {
      val future = ask(k, GetLock(clientID, v))
      val done = Await.result(future, timeout.duration).asInstanceOf[AckMsg]
      if (done.par) { // means it is in a partition
        clearClient() // clear itself and tell all store servers about his keys
        multicastPartition()
        return false
      }  else if (!done.result) {
        return false
      } else {
        for (op <- v) {
          if (!locksHolder.contains(op.key)) {
            locksHolder += op.key
          }
        }
      }
    }

    for (op <- opsLog) {
      if (cache.contains(op.key)) {
        cache -= op.key
      }
    }
    println(s"client ${clientID} has got all required locks which are: ${locksHolder}")
    return true
  }

  /** Data unlock **/
  def unLock(lockHolder: scala.collection.mutable.ArrayBuffer[BigInt]): Unit = {
    println(s"client ${clientID} try to unlock incomplete locks in acquire phase")
    for (lock <- lockHolder) {
        println(s"client ${clientID} try to unlock ${lock} in acquire phase")
        val future = ask(route(lock), UnLock(clientID, lock))
        val done = Await.result(future, timeout.duration).asInstanceOf[AckMsg]
        if (done.par) {
          clearClient()
          multicastPartition()
        }
        if (done.result) {
          println(s"client ${clientID} unlock ${lock} in acquire phase successfully")
        } else {
          println(s"client ${clientID} unlock ${lock} in acquire phase failure")
        }
    }
    lockHolder.clear()
  }


//  /** Cached read */
//  def read(key: BigInt): Option[Any] = {
//    var value: Int = cache.get(key)
//    if (value.isEmpty) {
//      value = directRead(key)
//      if (value.isDefined)
//        cache.put(key, value.get)
//    }
//    value
//  }

  /** Cached write: place new value in the local cache, record the update in dirtyset. */
  def write(key: BigInt, value: Int, dirtyset: IntMap) = {
    cache.put(key, value)
    dirtyset.put(key, value)
  }

  /** Push a dirtyset of cached writes through to the server. */
  def push(dirtyset: IntMap) = {
    val futures = for ((key, v) <- dirtyset)
      directWrite(key, v)
    dirtyset.clear()
  }

  /** Purge every value from the local cache.  Note that dirty data may be lost: the caller
    * should push them.
    */
  def purge() = {
    cache.clear()
  }

  /** Direct read, bypass the cache: always a synchronous read from the store, leaving the cache unchanged. */
  def directRead(key: BigInt): Int = {
    val future = ask(route(key), Get(key)).mapTo[Int]
    Await.result(future, timeout.duration)
  }

  /** Direct write, bypass the cache: always a synchronous write to the store, leaving the cache unchanged. */
  def directWrite(key: BigInt, value: Int) = {
    val future = ask(route(key), Put(key,value)).mapTo[Option[Any]]
    Await.result(future, timeout.duration)
  }

  /** clear entry, called by ring server when their cached data is modified by others */
  def clearEntry(key: BigInt) = {
    cache -= key
  }

  /** clear client, triggered when server tell me that i failed before and my resources are reclaimed*/
  def clearClient() = {
    println(s"client ${clientID} is cleaned")
    opsLog.clear()
    locksHolder.clear()
    votesTable.clear()
    commitTable.clear()
    acquireTable.clear()
    heartbeatTable.clear()
    cache = snapshotCache
    println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[35mCache info: client ${clientID} transaction failure (partitioned): cache is: ${cache} \033[0m")

  }

  import java.security.MessageDigest

  /** Generates a convenient hash key for an object to be written to the store.  Each object is created
    * by a given client, which gives it a sequence number that is distinct from all other objects created
    * by that client.
    */
  def hashForKey(nodeID: Int, cellSeq: Int): BigInt = {
    val label = "Node" ++ nodeID.toString ++ "+Cell" ++ cellSeq.toString
    val md: MessageDigest = MessageDigest.getInstance("MD5")
    val digest: Array[Byte] = md.digest(label.getBytes)
    BigInt(1, digest)
  }

  /**
    * @param key A key
    * @return An ActorRef for a store server that stores the key's value.
    */
  private def route(key: BigInt): ActorRef = {
    stores((key % stores.length).toInt)
  }
}
