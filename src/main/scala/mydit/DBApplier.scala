package mydit

import com.github.shyiko.mysql.binlog.event.WriteRowsEventData
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData

trait DBApplier {
   def binlogGetPosition: Option[(String, Long)]
   def binlogRotate(filename: String, position: Long)
   def binlogNextPosition(position: Long)
   def insert(nextPosition: Long, dbName: String, collName: String, cols: Seq[ColInfo], data: WriteRowsEventData)
   def update(nextPosition: Long, dbName: String, collName: String, cols: Seq[ColInfo], data: UpdateRowsEventData)
   def remove(nextPosition: Long, dbName: String, collName: String, cols: Seq[ColInfo], data: DeleteRowsEventData)
}