package akka.persistence.hbase.journal

import akka.persistence.hbase.HBaseClientFactory
import akka.persistence.hbase.snapshot.HBaseSnapshotConfig
import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory

object HBaseJournalTCKSpecConfig {
  // because of costy init of hbase-client, afterwards it's fast
  val config = ConfigFactory.parseString("akka.test.timefactor=5").withFallback(ConfigFactory.load())

}
/**
 * Plugin TCK (Martin's) Spec
 */
class HBaseJournalTCKSpec extends JournalSpec(HBaseJournalTCKSpecConfig.config) {

  override def supportsRejectingNonSerializableObjects = false

  val journalConfig = new HBaseJournalConfig(config)
  val snapshotConfig = new HBaseSnapshotConfig(config)

  override protected def beforeAll() {
    HBaseJournalInit.createTable(config, journalConfig.table, journalConfig.family)
    HBaseJournalInit.createTable(config, snapshotConfig.snapshotTable, snapshotConfig.snapshotFamily)

    super.beforeAll()
  }

  override protected def afterAll() {
    HBaseJournalInit.disableTable(config, journalConfig.table)
    HBaseJournalInit.deleteTable(config, journalConfig.table)

    HBaseJournalInit.disableTable(config, snapshotConfig.snapshotTable)
    HBaseJournalInit.deleteTable(config, snapshotConfig.snapshotTable)

    HBaseClientFactory.reset()

    super.afterAll()
  }

}
