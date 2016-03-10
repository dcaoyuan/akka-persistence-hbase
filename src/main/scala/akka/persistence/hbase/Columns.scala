package akka.persistence.hbase

import org.apache.hadoop.hbase.util.Bytes._

object Columns {
  val PERSISTENCE_ID = toBytes("persistenceId")
  val SEQUENCE_NR = toBytes("sequenceNr")
  val MARKER = toBytes("marker")
  val MESSAGE = toBytes("payload")

  val EVENT = toBytes("event")
  val SER_ID = toBytes("ser_id")
  val SER_MANIFEST = toBytes("ser_manifest")
  val EVENT_MANIFEST = toBytes("event_manifest")
  val WRITER_UUID = toBytes("writer_uuid")
}