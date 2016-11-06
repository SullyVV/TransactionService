package rings

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.{Actor, ActorRef, Props}

import scala.collection.mutable
import scala.concurrent.Await

sealed trait KVStoreAPI
// default lockOwner ID is -1
class StoredData(var key: BigInt, var value: Int, var cacheOwner: scala.collection.mutable.ArrayBuffer[Int], var lockOwner: Int)
/**
 * KVStore is a local key-value store based on actors.  Each store actor controls a portion of
 * the key space and maintains a hash of values for the keys in its portion.  The keys are 128 bits
 * (BigInt), and the values are of type Any.
 */
// write means + 1 ---> easy to test
class KVStore extends Actor {
  // get all an array of all clients' ActorRef
  private val store = new scala.collection.mutable.HashMap[BigInt, StoredData]
  // in writeLog, transaction ID = clientID, here we consider each client to be single threaded
  private val writeLog = new mutable.HashMap[Int, scala.collection.mutable.ArrayBuffer[WriteElement]]
  val generator = new scala.util.Random
  var endpoints: Option[Seq[ActorRef]] = None
  private val dateFormat = new SimpleDateFormat ("mm:ss")
  override def receive = {
    case View(k) =>
      endpoints = Some(k)
    case GetLock(clientID, opsArray) =>
      if(lock(clientID, opsArray)) {
        // return success
        sender() ! true
      } else {
        // return fail
        sender ! false
      }
    case UnLock(clientID, key) =>
      sender ! unlock(clientID, key)
    case Commit(clientID, writeArray) =>
      // commit or not depends on whether write log successfully, lets see 95% success and 5% failure
      val rand = generator.nextInt(100)
      if (rand > 95) { // fail
        sender ! false
      }  else {// success
        writeLog.put(clientID, writeArray)
        sender ! true
      }
    case CommitDecision(clientID, decision) =>
      notificationHandler(clientID, decision)
    case Put(key, value) =>
      val tmp = store(key)
      tmp.value = value
      sender ! store.put(key, tmp)
    case Get(key) =>
      sender ! store(key).value
  }


  def notificationHandler(clientID: Int, decision: Boolean)= {
    if (decision == true && writeLog.contains(clientID)) {
      // if decision is true, write to disk
      for (currWriteElement <- writeLog(clientID)) {
        multicast(clientID, store(currWriteElement.key).cacheOwner, currWriteElement.key)
        store(currWriteElement.key).cacheOwner.clear()
        store(currWriteElement.key).cacheOwner += clientID
        store(currWriteElement.key).value = currWriteElement.value
      }
    }
    // unlock the values because current client are done with them
    for ((k, v) <- store) {
      if (v.lockOwner == clientID) {
        v.lockOwner = -1
        println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[33mSuccess: client $clientID unlock key: ${k} in notify phase\033[0m")
      }
    }
    // clear this transaction's entry in writeLog
    writeLog -= clientID
  }

  def multicast(clientID: Int, cacheHolder: scala.collection.mutable.ArrayBuffer[Int], key: BigInt) = {
    // TODO: DirtyData in ringServer
    //we assume client stays up at this moment
    val list = endpoints.get
    for (i <- cacheHolder) {
      if (i != clientID)
      list(i) ! DirtyData(key)
    }
  }

  def lock(clientID: Int, opsArray: scala.collection.mutable.ArrayBuffer[Operation]): Boolean = {
    // we must gurantee "all or none" on all lock request for one transaction on one server
    // check first
    for (op <- opsArray) {
      if (store.contains(op.key) && store(op.key).lockOwner != -1) {
        return false
      }
    }
    // assign lock second
    for (op <- opsArray) {
      if (!store.contains(op.key)) {
        store.put(op.key, new StoredData(op.key, 0, new scala.collection.mutable.ArrayBuffer[Int], -1))
      }
      if (store(op.key).lockOwner == -1) {
        println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[44mSuccess: client $clientID lock key: ${op.key}\033[0m")
        store(op.key).lockOwner = clientID
      } else {
        println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[36mSuccess: client $clientID already has key: ${op.key}\033[0m")
      }
    }
    return true
  }

  def unlock(clientID: Int, key: BigInt): Boolean = {
    store(key).lockOwner = -1
    println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[33mSuccess: client $clientID unlock key: ${key} in acquire phase\033[0m")
    return true
  }
}

object KVStore {
  def props(): Props = {
     Props(classOf[KVStore])
  }
}


