package rings

import java.text.SimpleDateFormat
import java.util.Date

import scala.concurrent.duration._
import scala.concurrent.Await
import akka.actor.ActorRef
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.mutable

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


/**   0. when do ops in cache
  *   1. when collection votes
  **/
class KVClient (clientID: Int, stores: Seq[ActorRef]) {
  private var cache = new IntMap
  private val snapshotCache = new IntMap
  implicit val timeout = Timeout(5 seconds)
  private val opsLog = new scala.collection.mutable.ArrayBuffer[Operation]
  private val locksHolder = new scala.collection.mutable.ArrayBuffer[BigInt]
  private val votesTable = new mutable.HashMap[ActorRef, Boolean]
  private var oID = 0
  private val dateFormat = new SimpleDateFormat ("mm:ss")
  private val commitTable = new mutable.HashMap[ActorRef, scala.collection.mutable.ArrayBuffer[WriteElement]]
  private val acquireTable = new mutable.HashMap[ActorRef, scala.collection.mutable.ArrayBuffer[Operation]]
  import scala.concurrent.ExecutionContext.Implicits.global

  /** transaction begin */
  def begin() = {
    // create a snapshot when begin
    // record begin and commit in case client fails when doing ops in cache
    println(s"client$clientID transaction begins")
    for ((k,v) <- cache) {
      snapshotCache.clear()
      snapshotCache.put(k, v)
    }
    println(s"client$clientID snapshot is $snapshotCache")
    opsLog += new Operation(oID, 2, -1)
    oID = oID + 1
  }

  /** transaction read */
  def transactionRead(key: BigInt) = {
    println(s"client$clientID read key: $key")
    opsLog += new Operation(oID, 0, key)
    oID = oID + 1
  }

  /** transaction write */
  def transactionWrite(key: BigInt) = {
    println(s"client$clientID write key: $key")
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
    oID = 0
  }

  /** transaction commit */
  def transactionCommit(): Boolean = {
    // acquire locks of all involved data, single fail->abort, 2PL
    opsLog += new Operation(oID, 3, -1)
    oID = oID + 1
    println(s"client$clientID commit")
    groupAcquires(opsLog)
    if (!Lock(acquireTable)) {
      println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[31mFailed: client ${clientID} failed in acquire locks\033[0m")
      unLock(locksHolder)
      cleanUp()
      return false
    } else {
      println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[32mSuccess: client ${clientID} success in acquire locks\033[0m")
    }
    // after acquire locks of all involved keys, do ops in local cache
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
    for ((k,v) <- commitTable) {
      val future = ask(k, Commit(clientID, v))
      val done = Await.result(future, timeout.duration).asInstanceOf[Boolean]
      votesTable.put(k, done)
    }

    // traverse all element in votesTable, if find any false, abort
    for ((k,v) <- votesTable) {
      if (!v) {
        // inform all participants to abort
        notifyParticipants(clientID, votesTable, false) // we should piggy back the unlock info with the notification
        // recover to the snapshot
        cache = snapshotCache
        cleanUp()
        println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[31mError: client: $clientID, participants ${k} votes for abort\033[0m")
        return false
      }
    }
    notifyParticipants(clientID, votesTable, true)
    cleanUp()
    println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[35mCache info: client ${clientID} cache is: ${cache} \033[0m")
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
  def notifyParticipants(clientID: Int, voteTable: mutable.HashMap[ActorRef, Boolean], decision: Boolean) = {
    for ((k,v) <- voteTable) {
      k ! CommitDecision(clientID, decision)
    }
  }

  /** Data lock **/
  def Lock(acquireTable: mutable.HashMap[ActorRef, scala.collection.mutable.ArrayBuffer[Operation]]): Boolean = {
    for ((k,v) <- acquireTable) {

      val future = ask(k, GetLock(clientID, v))
      val done = Await.result(future, timeout.duration).asInstanceOf[Boolean]
      if (done == false) {
        return false
      } else {
        for (op <- v) {
          if (!locksHolder.contains(op.key)) {
            locksHolder += op.key
          }
        }
      }
    }
    println(s"client ${clientID} has got all required locks which are: ${locksHolder}")
    return true
  }

  /** Data unlock **/
  def unLock(lockHolder: scala.collection.mutable.ArrayBuffer[BigInt]): Unit = {
    println(s"client ${clientID} try to unlock incomplete locks in acquire phase")
    println(lockHolder)
    for (lock <- lockHolder) {
        println(s"client ${clientID} try to unlock ${lock} in acquire phase")
        val future = ask(route(lock), UnLock(clientID, lock))
        val done = Await.result(future, timeout.duration).asInstanceOf[Boolean]
        if (done == true) {
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
    println(s"client $clientID cleared entry for key: $key")
    println(s"client $clientID new cache is: $cache")
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
