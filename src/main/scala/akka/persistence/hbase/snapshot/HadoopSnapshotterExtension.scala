package akka.persistence.hbase.snapshot

import akka.actor.ExtendedActorSystem
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.persistence.hbase.HBaseClientFactory
import akka.persistence.hbase.journal.HBaseJournalInit

object HadoopSnapshotterExtension extends ExtensionId[HadoopSnapshotter] with ExtensionIdProvider {

  val SnapshotStoreModeKey = "hadoop-snapshot-store.mode"

  override def lookup() = HadoopSnapshotterExtension

  override def createExtension(system: ExtendedActorSystem) = {
    val config = system.settings.config
    val mode = config.getString(SnapshotStoreModeKey)

    val pluginPersistenceSettings = new HBaseSnapshotConfig(config)

    val client = HBaseClientFactory.getClient(pluginPersistenceSettings)

    mode match {
      case "hbase" =>
        system.log.info("Using {} snapshotter implementation", classOf[HBaseSnapshotter].getCanonicalName)
        HBaseJournalInit.createTable(config, pluginPersistenceSettings.snapshotTable, pluginPersistenceSettings.snapshotFamily)
        new HBaseSnapshotter(system, pluginPersistenceSettings, client)

      case "hdfs" =>
        system.log.info("Using {} snapshotter implementation", classOf[HdfsSnapshotter].getCanonicalName)
        new HdfsSnapshotter(system, pluginPersistenceSettings)

      case other =>
        throw new IllegalStateException(s"$SnapshotStoreModeKey must be set to either ${classOf[HBaseSnapshotter].getCanonicalName} or ${classOf[HdfsSnapshotter].getCanonicalName}! Was: $other")
    }
  }
}
