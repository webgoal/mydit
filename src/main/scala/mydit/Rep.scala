package mydit

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import scala.collection.mutable.Queue
import scala.util.control.NonFatal


/**
 * Error handling:
 *
 * When there's problem, it will be logged.
 *
 * When there's problem replicating to MongoDB, the replicator will queue the
 * data and try to replicate it later. If the queue size exceeds the configured
 * threshold, the replicator process will exit.
 */
class Rep(config: Config) extends RepEvent.Listener {
  
  private val mo = new OutputDBApplier(
    config.mongoUri, config.mongoBinlogDb, config.mongoBinlogColl,
    config.enumToString
  )
    
//  private val mo = new MongoDBApplier(
//    config.mongoUri, config.mongoBinlogDb, config.mongoBinlogColl,
//    config.enumToString
//  )

  private val failedEventQ  = Queue[RepEvent.Event]()
  private val singleThreadE = Executors.newSingleThreadExecutor()

  val my = new MySQLExtractor(
    config.myHost, config.myPort, config.myUsername, config.myPassword, config.myOnly,
    mo.binlogGetPosition
  )
  my.addListener(this)
  my.connectKeepAlive()

  //--------------------------------------------------------------------------

  override def onEvent(event: RepEvent.Event) {
    // Queue the event to be processed one by one because the replication
    // must be in order. For better performance, we run in a separate thread
    // to avoid blocking the MySQL binlog event reader thread.
    singleThreadE.execute(new Runnable {
      override def run() {
        processEvent(event)
      }
    })
  }

  private def processEvent(event: RepEvent.Event) {
    val lastQSize = failedEventQ.size

    // Replicate things in the queue first
    var ok = true
    while (ok && failedEventQ.nonEmpty) {
      val failedEvent = failedEventQ.front
      ok = replicate(failedEvent)
      if (ok) failedEventQ.dequeue()
    }

    // Replicate this latest event if the above are replicated ok
    if (ok) ok = replicate(event)

    if (!ok) failedEventQ.enqueue(event)

    val newQSize = failedEventQ.size
    if (newQSize > 0) {
      Log.info("Failed replication event queue size: {}", newQSize)
      if (newQSize > config.maxFailedEventQueueSize) {
        Log.error(
          "Replicator program now exits because the failed replication event queue size exceeds {} (see config/application.conf)",
          config.maxFailedEventQueueSize
        )
        my.disconnectAndExit()
      }
    } else if (lastQSize > 0) {
      // newQSize is now 0, congratulations!
      Log.info("Failed replication event queue is now empty, before: {}", lastQSize)
    }
  }

  /** @return false on any Exception */
  private def replicate(event: RepEvent.Event): Boolean = {
    try {
      event match {
        case e: RepEvent.BinlogRotate =>
          mo.binlogRotate(e.filename, e.position)

        case e: RepEvent.BinlogNextPosition =>
          mo.binlogNextPosition(e.position)

        case e: RepEvent.Insert =>
          mo.insert(e.nextPosition, e.ti.data.getDatabase, e.ti.data.getTable, e.ti.cols, e.data)

        case e: RepEvent.Update =>
          mo.update(e.nextPosition, e.ti.data.getDatabase, e.ti.data.getTable, e.ti.cols, e.data)

        case e: RepEvent.Remove =>
          mo.remove(e.nextPosition, e.ti.data.getDatabase, e.ti.data.getTable, e.ti.cols, e.data)
      }
      true
    } catch {
      case NonFatal(e) =>
        Log.warn("Could not replicate to MongoDB, event: " + event, e)
        false
    }
  }
}
