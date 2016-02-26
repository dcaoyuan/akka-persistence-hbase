package akka.persistence.hbase

import akka.persistence.hbase.Columns._
import akka.persistence.hbase.DeferredConversions._
import akka.persistence.hbase.journal.HBaseJournalConfig
import akka.persistence.hbase.journal.RowTypeMarkers._
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.apache.hadoop.hbase.filter.KeyOnlyFilter
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.filter.RegexStringComparator
import org.apache.hadoop.hbase.filter.RowFilter
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.{ DeleteRequest, KeyValue, PutRequest }
import scala.concurrent.{ ExecutionContext, Future }

class Session(table: Array[Byte], family: Array[Byte], config: HBasePluginConfig)(implicit executionContext: ExecutionContext) {

  val client = HBaseClientFactory.getClient(config)
  lazy val htable = new HTable(config.hadoopConfiguration, table)

  /** Used to avoid writing all data to the same region - see "hot region" problem */
  def selectPartition(sequenceNr: Long)(implicit journalConfig: HBaseJournalConfig): Long =
    RowKey.selectPartition(sequenceNr)

  def isSnapshotRow(columns: Seq[KeyValue]): Boolean =
    java.util.Arrays.equals(findColumn(columns, Marker).value, SnapshotMarkerBytes)

  def findColumn(columns: Seq[KeyValue], qualifier: Array[Byte]): KeyValue =
    columns find { kv =>
      java.util.Arrays.equals(kv.qualifier, qualifier)
    } getOrElse {
      throw new RuntimeException(s"Unable to find [${Bytes.toString(qualifier)}}] field from: ${columns.map(kv => Bytes.toString(kv.qualifier))}")
    }

  def getColumnLatestCell(result: Result, qualifier: Array[Byte]) = {
    result.getColumnLatestCell(family, qualifier)
  }

  def deleteRow(key: Array[Byte]): Future[Unit] = {
    //      println(s"Permanently deleting row: ${Bytes.toString(key)}")
    executeDelete(key)
  }

  def markRowAsDeleted(key: Array[Byte]): Future[Unit] = {
    //      println(s"Marking as deleted, for row: ${Bytes.toString(key)}")
    executePut(key, Array(Marker), Array(DeletedMarkerBytes))
  }

  def executeDelete(key: Array[Byte]): Future[Unit] = {
    val request = new DeleteRequest(table, key)
    client.delete(request)
  }

  def executePut(key: Array[Byte], qualifiers: Array[Array[Byte]], values: Array[Array[Byte]]): Future[Unit] = {
    val request = new PutRequest(table, key, family, qualifiers, values)
    client.put(request)
  }

  /**
   * Sends the buffered commands to HBase. Does not guarantee that they "complete" right away.
   */
  def flushWrites() {
    client.flush()
  }

  def newScanner() = {
    val scanner = client.newScanner(table)
    scanner.setFamily(family)
    scanner
  }

  // --- moved from HBaseUtil

  def preparePartitionScan(startScanKey: RowKey, stopScanKey: RowKey, persistenceIdRowRegex: String, onlyRowKeys: Boolean): Scan = {
    val scan = new Scan
    scan.setStartRow(startScanKey.toBytes) // inclusive
    scan.setStopRow(stopScanKey.toBytes) // exclusive
    scan.setBatch(config.scanBatchSize)

    val filter = if (onlyRowKeys) {
      val fl = new FilterList()
      fl.addFilter(new FirstKeyOnlyFilter)
      fl.addFilter(new KeyOnlyFilter)
      fl.addFilter(new RowFilter(CompareOp.EQUAL, new RegexStringComparator(persistenceIdRowRegex)))
      fl
    } else {
      scan.addColumn(family, Marker)
      scan.addColumn(family, Message)

      new RowFilter(CompareOp.EQUAL, new RegexStringComparator(persistenceIdRowRegex))
    }

    scan.setFilter(filter)
    scan
  }

  /**
   * For snapshots
   */
  def preparePrefixScan(startScanKey: SnapshotRowKey, stopScanKey: SnapshotRowKey, persistenceIdPrefix: String, onlyRowKeys: Boolean): Scan = {
    preparePrefixScan(startScanKey.toBytes, stopScanKey.toBytes, persistenceIdPrefix, onlyRowKeys)
  }

  def preparePrefixScan(startScanKey: Array[Byte], stopScanKey: Array[Byte], persistenceIdPrefix: String, onlyRowKeys: Boolean): Scan = {
    val scan = new Scan
    scan.setStartRow(startScanKey) // inclusive
    scan.setStopRow(stopScanKey) // exclusive
    scan.setBatch(config.scanBatchSize)

    scan.setFilter(new PrefixFilter(Bytes.toBytes(persistenceIdPrefix)))

    scan
  }
}
