package akka.persistence.hbase

import akka.persistence.hbase.journal.HBaseJournalConfig
import org.apache.hadoop.hbase.util.Bytes

final case class RowKey(persistenceId: String, sequenceNr: Long)(implicit journalConfig: HBaseJournalConfig) {
  val partition = selectPartition(sequenceNr)

  def toBytes = Bytes.toBytes(toKeyString)

  def toKeyString = s"${padded(partition, 3)}-$persistenceId-${padded(sequenceNr, RowKey.lengthSequenceNr)}"

  /** Used to avoid writing all data to the same region - see "hot region" problem */
  private def selectPartition(sequenceNr: Long)(implicit journalConfig: HBaseJournalConfig): Int = {
    (sequenceNr % journalConfig.partitionCount).toInt + 1 // 1 ~ partitionCount
  }

  @inline
  def padded(l: Long, howLong: Int) = String.valueOf(l).reverse.padTo(howLong, "0").reverse.mkString
}

final case class SnapshotRowKey(persistenceId: String, sequenceNr: Long) {

  def toBytes = Bytes.toBytes(toKeyString)

  def toKeyString = s"$persistenceId-${padded(sequenceNr, RowKey.lengthSequenceNr)}"

  @inline
  def padded(l: Long, howLong: Int) = String.valueOf(l).reverse.padTo(howLong, "0").reverse.mkString
}

object RowKey {
  val lengthSequenceNr = 20

  /**
   * Since we're salting (prefixing) the entries with selectPartition numbers,
   * we must use this pattern for scanning for "all messages for processorX"
   */
  def patternForProcessor(persistenceId: String)(implicit journalConfig: HBaseJournalConfig) = s""".*-$persistenceId-.*"""

  def firstInPartition(persistenceId: String, fromSequenceNr: Long = 0)(implicit journalConfig: HBaseJournalConfig) = {
    RowKey(persistenceId, fromSequenceNr)
  }

  def lastInPartition(persistenceId: String, toSequenceNr: Long = Long.MaxValue)(implicit journalConfig: HBaseJournalConfig) = {
    require(toSequenceNr >= 0, s"toSequenceNr must be >= 0, ($toSequenceNr)")
    RowKey(persistenceId, toSequenceNr)
  }

  val RowKeyPattern = """\d+-.*-\d""".r

  //val num = rowKey.reverse.takeWhile(_.toChar.isDigit).reverse 
  def extractSeqNr(rowKey: Array[Byte]): Long = {
    var i = rowKey.length - 1
    var n = lengthSequenceNr - 1
    val num = Array.fill[Byte](lengthSequenceNr)(48) // filled with '0'
    while (i >= 0 && n >= 0) {
      val b = rowKey(i)
      if (b >= 48 && b <= 57) { // '0' - '9'
        num(n) = b
        n -= 1
        i -= 1
      } else {
        i = -1 // force to break
      }
    }
    Bytes.toString(num).toLong
  }
}

object SnapshotRowKey {

  def firstForPersistenceId(persistenceId: String): SnapshotRowKey =
    SnapshotRowKey(persistenceId, 0)

  def lastForPersistenceId(persistenceId: String, toSequenceNr: Long = Long.MaxValue): SnapshotRowKey =
    SnapshotRowKey(persistenceId, toSequenceNr)
}