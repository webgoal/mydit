package mydit

import java.io.Serializable
import java.math.BigDecimal
import java.util.BitSet

import scala.collection.JavaConverters._

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData

import com.mongodb.BasicDBObject
import com.mongodb.DBObject
import com.mongodb.DuplicateKeyException
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern

class OutputDBApplier(uri: String, binlogDb: String, binlogCollName: String, enumToString: Boolean) extends DBApplier {
  
  def binlogGetPosition: Option[(String, Long)] = {
    Option("mysql-bin.000001", 0L)
  }
  
  def binlogRotate(filename: String, position: Long) {
    println("-------------------------- binlogRotate --------------------------")
  }
  
  def binlogNextPosition(position: Long) {
    println("-------------------------- binlogNextPosition --------------------------")
  }

  def insert(nextPosition: Long, dbName: String, collName: String, cols: Seq[ColInfo], data: WriteRowsEventData) {
    println("-------------------------- insert --------------------------")
    for (mySQLValues <- data.getRows.asScala) {
      val obj = mySQLRowToMongoDBObject(cols, data.getIncludedColumns, mySQLValues)
      println(obj, WriteConcern.ACKNOWLEDGED)
    }
//    binlogNextPosition(nextPosition)
  }

  def update(nextPosition: Long, dbName: String, collName: String, cols: Seq[ColInfo], data: UpdateRowsEventData) {
    println("-------------------------- update --------------------------")
    for (entry <- data.getRows.asScala) {
      val before = mySQLRowToMongoDBObject(cols, data.getIncludedColumnsBeforeUpdate, entry.getKey)
      val after  = mySQLRowToMongoDBObject(cols, data.getIncludedColumns,             entry.getValue)
      println(before, new BasicDBObject("$set", after), true, false, WriteConcern.ACKNOWLEDGED)  // Upsert
    }
//    binlogNextPosition(nextPosition)
  }

  def remove(nextPosition: Long, dbName: String, collName: String, cols: Seq[ColInfo], data: DeleteRowsEventData) {
    Log.info("-------------------------- remove --------------------------")
    for (mySQLValues <- data.getRows.asScala) {
      val obj = mySQLRowToMongoDBObject(cols, data.getIncludedColumns, mySQLValues)
      println(obj, WriteConcern.ACKNOWLEDGED)
    }
//    binlogNextPosition(nextPosition)
  }

  //--------------------------------------------------------------------------

  private def mySQLRowToMongoDBObject(cols: Seq[ColInfo], includedColumns: BitSet, mySQLValues: Array[Serializable]): DBObject = {
    val ret = new BasicDBObject
    for (i <- 0 until cols.size) {
      if (includedColumns.get(i)) {
        val ci           = cols(i)
        val mongoDBValue = mySQLValueToMongoDBValue(ci, mySQLValues(i))
        ret.put(ci.name, mongoDBValue)
      }
    }
    ret
  }

  private def mySQLValueToMongoDBValue(ci: ColInfo, value: Serializable): Serializable = {
    // Convert MySQL enum to MongoDB String
    if (enumToString && ci.typeLowerCase == "enum") {
      val id = value.asInstanceOf[Int]  // Starts from 1; 0 if the enum is null
      return if (id <= 0) null else ci.enumValues(id - 1)
    }

    // MongoDB doesn't support BigDecimal, convert it to Double
    if (value.isInstanceOf[BigDecimal]) {
      val b = value.asInstanceOf[BigDecimal]
      return b.doubleValue
    }

    // Text is passed as byte array, covert it to UTF-8 String
    if (ci.typeLowerCase == "text") {
      val bytes = value.asInstanceOf[Array[Byte]]
      return new String(bytes, "UTF-8")
    }

    return value
  }
}
