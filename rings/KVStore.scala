package rings

import akka.actor.{Actor, ActorRef, Props}

import scala.collection.mutable
import scala.concurrent.Await

sealed trait KVStoreAPI
// default lockOwner ID is -1
class StoredData(var key: BigInt, var value: Int, var cacheOwner: scala.collection.mutable.ArrayBuffer[Int], var lockOwner: Int)
class writeElement(val key: BigInt, var value: Int)
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
  private val writeLog = new mutable.HashMap[Int, scala.collection.mutable.ArrayBuffer[writeElement]]
  val generator = new scala.util.Random
  var endpoints: Option[Seq[ActorRef]] = None


  override def receive = {
    case View(k) =>
      endpoints = Some(k)
    case GetLock(clientID, key) =>
      if(lock(clientID, key)) {
        // return success
        sender() ! true
      } else {
        // return fail
        sender ! false
      }
    case UnLock(key) =>
      sender ! unlock(key)
    case Commit(clientID, key, value) =>
      // commit or not depends on whether write log successfully, lets see 95% success and 5% failure
      val rand = generator.nextInt(100)
      if (rand > 95) {
        // fail
        sender ! false
      }  else {
        // success
        if (!writeLog.contains(clientID)) {
          writeLog.put(clientID, new scala.collection.mutable.ArrayBuffer[writeElement])
        }
        writeLog(clientID) += new writeElement(key, value)
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
    if (decision == true) {
      // if decision is true, write to disk
      val currWriteLog = writeLog(clientID)
      for (currWriteElement <- currWriteLog) {
        multicast(store(currWriteElement.key).cacheOwner, currWriteElement.key)
        store(currWriteElement.key).cacheOwner.clear()
        store(currWriteElement.key).cacheOwner += clientID
        store(currWriteElement.key).value = currWriteElement.value
      }
    }
    // clear this transaction's entry in writeLog
    writeLog -= clientID
  }

  def multicast(cacheHolder: scala.collection.mutable.ArrayBuffer[Int], key: BigInt) = {
    // TODO: DirtyData in ringServer
    //we assume client stays up at this moment
    for (i <- cacheHolder) {
      val list = endpoints.get
      list(i) ! DirtyData(key)
    }
  }

  def lock(clientID: Int, key: BigInt): Boolean = {
    if (!store.contains(key)) {
      // default value starts with 0
      store.put(key, new StoredData(key, 0, new scala.collection.mutable.ArrayBuffer[Int], -1))
    }
    if (store(key).lockOwner != -1) {
      // ask original lockOwner see if is still live (it is possible that client fails after getting locks)
      // this is a typical problem in 2PL --> safe but not alive
      return false
    } else {
      store(key).lockOwner = clientID
      return true
    }

  }

  def unlock(key: BigInt): Boolean = {
    store(key).lockOwner = -1
    return true
  }
}

object KVStore {
  def props(): Props = {
     Props(classOf[KVStore])
  }
}


