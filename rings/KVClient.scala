package rings

import scala.concurrent.duration._
import scala.concurrent.Await
import akka.actor.ActorRef
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
/**
 * KVClient implements a client's interface to a KVStore, with an optional writeback cache.
 * Instantiate one KVClient for each actor that is a client of the KVStore.  The values placed
 * in the store are of type Any: it is up to the client app to cast to/from the app's value types.
 **/


/**   0. when do ops in cache
  *   1. when collection votes
  **/
class KVClient (clientID: Int, stores: Seq[ActorRef]) {
  private val cache = new IntMap
  private val snapshotCache = new IntMap
  implicit val timeout = Timeout(5 seconds)
  private val opsLog = new scala.collection.mutable.ArrayBuffer[Operation]
  private val resLog = new scala.collection.mutable.HashMap[Int, OpsResult]
  private val locksHolder = new scala.collection.mutable.ArrayBuffer[BigInt]
  private val votesTable = new mutable.HashMap[BigInt, Boolean]
  private var oID = 0
  import scala.concurrent.ExecutionContext.Implicits.global

  /** transaction begin */
  def begin() = {
    // create a snapshot when begin
    // record begin and commit in case client fails when doing ops in cache
    for ((k,v) <- cache) {
      snapshotCache.put(k, v)
    }
    opsLog += new Operation(oID, 2, -1)
    println(s"Transaction begins")
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
    cleanUp()
  }

  /** clean up **/
  def cleanUp() = {
    unLock(opsLog)
    opsLog.clear()
    votesTable.clear()
    oID = 0
  }

  /** transaction commit */
  def transactionCommit(): Boolean = {
    // acquire locks of all involved data, single fail->abort, 2PL
    if (!Lock(opsLog)) {
      cleanUp()
      return false
    }
    // after acquire locks of all involved keys, do ops on local cache
    for (i <- 0 until opsLog.size) {
      val currentOperation = opsLog(i)
      if (currentOperation.ops == 0 || currentOperation.ops == 1) {
        var tmp = getCurrValue(currentOperation.key)
        if (currentOperation.ops == 1) {
          tmp = tmp + 1
        }
        cache.put(currentOperation.key, tmp)
      }
    }
    // after all operations in local cache, now we need to do 2PC for all write ops, get votes from all participants
    for (i <- 0 until opsLog.size) {
      val key = opsLog(i).key
      if (opsLog(i).ops == 1) {
        // handle write operation only
        val value = cache(key)
        val future = ask(route(key), Commit(clientID, key, value))
        val done = Await.result(future, timeout.duration).asInstanceOf[Boolean]
        // here we must use a data structure to keep the votes from everyone
        votesTable.put(key, done)
      } else {
        // for read operation, no need for votes, default true
        votesTable.put(key, true)
      }
    }
    // traverse all element in votesTable, if find any false, abort
    for ((k,v) <- votesTable) {
      if (v == false) {
        // inform all participants to abort
        notifyParticipants(opsLog, false)
        cleanUp()
        return false
      }
    }
    notifyParticipants(opsLog, true)
    cleanUp()
    return true
  }

  /** get current value of the key **/
  def getCurrValue(key: BigInt) : Int = {
    var tmp: Int = 0
    if (cache.contains(key)) {
      // cache has data
      tmp = cache(key)
    } else {
      // cache doesnt have data, go get the data from server
      tmp = directRead(key).get
      if (tmp == None) {
        tmp = 0
      }
      cache.put(key, tmp)
    }
    return tmp
  }

  /** Notify all participants about the result **/
  def notifyParticipants(opsLog: scala.collection.mutable.ArrayBuffer[Operation], decision: Boolean) = {
    // TODO: CommitDecision msg in server
    for (i <- 0 until opsLog.size) {
      if (opsLog(i).ops == 1) {
        // only write ops need commit, so only write ops need notification
        route(opsLog(i).key) ! CommitDecision(clientID, decision)
      }
    }
  }

  /** Data lock **/
  def Lock(opsLog: scala.collection.mutable.ArrayBuffer[Operation]): Boolean = {
    // TODO: GetLock msg in server
    for (i <- 0 until opsLog.size) {
      if (opsLog(i).ops == 0 || opsLog(i).ops== 1) {
        val future = ask(route(opsLog(i).key), GetLock(clientID, opsLog(i).key))
        val done = Await.result(future, timeout.duration).asInstanceOf[Boolean]
        if (done == false) {
          return false
        } else {
          locksHolder += opsLog(i).key
        }
      }
    }
    return true
  }

  /** Data unlock **/
  def unLock(opsLog: scala.collection.mutable.ArrayBuffer[Operation]): Unit = {
    for (i <- 0 until opsLog.size) {
      if (opsLog(i).key != -1) {
        val future = ask(route(opsLog(i).key), UnLock(opsLog(i).key))
        val done = Await.result(future, timeout.duration).asInstanceOf[Boolean]
        if (done == true) {
          locksHolder -= opsLog(i).key
        }
      }
    }
  }

  /** Cached read */
  def read(key: BigInt): Option[Any] = {
    var value = cache.get(key)
    if (value.isEmpty) {
      value = directRead(key)
      if (value.isDefined)
        cache.put(key, value.get)
    }
    value
  }

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
  def directRead(key: BigInt): Option[Int] = {
    val future = ask(route(key), Get(key)).mapTo[Option[Int]]
    Await.result(future, timeout.duration)
  }

  /** Direct write, bypass the cache: always a synchronous write to the store, leaving the cache unchanged. */
  def directWrite(key: BigInt, value: Any) = {
    val future = ask(route(key), Put(key,value)).mapTo[Option[Any]]
    Await.result(future, timeout.duration)
  }

  /** clear entry, called by ring server when their cached data is modified by others */
  def clearEntry(key: BigInt) = {
    cache -= key
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
