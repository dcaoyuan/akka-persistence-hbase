package akka.persistence.hbase.snapshot

import com.typesafe.config.Config
import akka.persistence.hbase.HBasePluginConfig

class HBaseSnapshotConfig(config: Config) extends HBasePluginConfig(config) {
  val snapshotTable = snapshotConfig.getString("hbase.table")
  val snapshotFamily = snapshotConfig.getString("hbase.family")
  val snapshotHdfsDir = snapshotConfig.getString("snapshot-dir")
}