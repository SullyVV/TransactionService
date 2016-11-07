package rings

import akka.actor.{Actor, ActorRef, Props}
import java.text.SimpleDateFormat
import java.util.Date

import scala.concurrent.duration._
import scala.concurrent.Await
import akka.actor.ActorRef
import akka.event.Logging
import akka.io.Tcp.SO.KeepAlive
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.mutable

sealed trait KVStoreAPI
class StoredData(var key: BigInt, var value: Int, var cacheOwner: scala.collection.mutable.ArrayBuffer[Int], var lockOwner: Int)   // default lockOwner ID is -1
class AckMsg(var result: Boolean, var par: Boolean)
/**
 * KVStore is a local key-value store based on actors.  Each store actor controls a portion of
 * the key space and maintains a hash of values for the keys in its portion.  The keys are 128 bits
 * (BigInt), and the values are of type Any.
 */
/**
  * for handling client partition, for lock request for taken locks, store server first check current holder's latest heartbeat.
  * if (current time - heartbeat) < period, means current holder is still valid, refuse this request
  * if (current time - heartbeat) > period, means current holder isnt valid. reclaim all resources occupied by the current clientID. (locks, data).
  *                                         put old lock holder into partitioned list
  *                                         all msg from client has to go through this list first, if the client is in the list, means it is partitioned and still in the list, then tell the client it is partitioned
  *                                         use a scheduler to tell the clients in the list repeatedly
  *
  *
  */
// write means + 1 ---> easy to test
class KVStore extends Actor {
  implicit val timeout = Timeout(5 seconds)
  // get all an array of all clients' ActorRef
  private val store = new scala.collection.mutable.HashMap[BigInt, StoredData]
  // in writeLog, transaction ID = clientID, here we consider each client to be single threaded
  private val writeLog = new mutable.HashMap[Int, scala.collection.mutable.ArrayBuffer[WriteElement]]
  private val partitionedList = new mutable.ArrayBuffer[Int]
  val generator = new scala.util.Random
  var endpoints: Option[Seq[ActorRef]] = None
  val heartbeatTable = new mutable.HashMap[Int, Long]
  val validPeriod:Long = 100
  private val dateFormat = new SimpleDateFormat ("mm:ss")
  override def receive = {
    case PartitionedClient(clientID) => {
      if (writeLog.contains(clientID)) {
        // writelog contains clientID means that this client is still valid in that server
        reclaimHandler(clientID)
      }
    }
    case HeartBeat(clientID) =>
      if (partitionedList.contains(clientID)) {
        val list = endpoints.get
        list(clientID) ! Partitioned()
        partitionedList -= clientID
      } else {
        heartbeatTable(clientID) = System.currentTimeMillis()
      }
      //heartbeatTable(clientID) = System.currentTimeMillis()
    case View(k) =>
      endpoints = Some(k)
    case GetLock(clientID, opsArray) =>
      if (partitionedList.contains(clientID)) {
        partitionedList -= clientID
        sender ! new AckMsg(false, true)
      } else if(lock(clientID, opsArray)) {
        sender ! new AckMsg(true, false)
      } else {
        sender ! new AckMsg(false, false)
      }
    case UnLock(clientID, key) =>
      if (partitionedList.contains(clientID)) {
        partitionedList -= clientID
        sender ! new AckMsg(false, true)
      } else if (unlock(clientID, key)) {
        sender ! new AckMsg(true, false)
      } else {
        sender ! new AckMsg(false, false)
      }
    case Commit(clientID, writeArray) =>
      // commit or not depends on whether write log successfully, lets see 95% success and 5% failure
      if (partitionedList.contains(clientID)) {
        partitionedList -= clientID
        sender ! new AckMsg(false, true)
      } else {
        val rand = generator.nextInt(100)
        if (rand > 95) { // fail
          sender ! new AckMsg(false, false)
        }  else {// success
          writeLog.put(clientID, writeArray)
          sender ! new AckMsg(true, false)
        }
      }


    case CommitDecision(clientID, decision) =>
      // commit decision not in ask pattern
      if (partitionedList.contains(clientID)) {
        val list = endpoints.get
        list(clientID) ! Partitioned()
        partitionedList -= clientID
      } else {
        notificationHandler(clientID, decision)
      }

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

  def reclaimHandler(clientID: Int): Unit = {
    for ((k,v) <- store) {
      if (v.lockOwner == clientID) {
        v.lockOwner = -1
        println(s"${dateFormat.format(new Date(System.currentTimeMillis()))}: \033[33mSuccess: client $clientID unlock key: ${k} in reclaim phase\033[0m")
      }
    }
    heartbeatTable -= clientID
    writeLog -= clientID

  }

  def multicast(clientID: Int, cacheHolder: scala.collection.mutable.ArrayBuffer[Int], key: BigInt) = {
    // TODO: DirtyData in ringServer
    //we assume client stays up at this moment
    val list = endpoints.get
    for (i <- cacheHolder) {
      if (i != clientID) {
        list(i) ! DirtyData(key)
      }
    }
  }

  def lock(clientID: Int, opsArray: scala.collection.mutable.ArrayBuffer[Operation]): Boolean = {
    // we must gurantee "all or none" on all lock request for one transaction on one server
    // check first, in this section, we also have to check if the original holder is alive
    for (op <- opsArray) {
      if (store.contains(op.key) && store(op.key).lockOwner != -1) {
        val currentLockOwner = store(op.key).lockOwner
        if (!heartbeatTable.contains(currentLockOwner) || System.currentTimeMillis() - heartbeatTable(currentLockOwner) < validPeriod) {
          return false
        } else {
          // exceeds validPeriod, old holder is partitioned, reclaim all its locks and clear its write log
          reclaimHandler(clientID)
          partitionedList += clientID
        }
      }
    }

//    for (op <- opsArray) {
//      if (store.contains(op.key) && store(op.key).lockOwner != -1) {
//        return false
//      }
//    }
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


