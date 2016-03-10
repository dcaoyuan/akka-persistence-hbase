package akka.persistence.hbase.snapshot

import akka.persistence.hbase.HBasePluginConfig
import com.typesafe.config.Config

class HBaseSnapshotConfig(config: Config) extends HBasePluginConfig(config) {
  val snapshotTable = snapshotConfig.getString("hbase.table")
  val snapshotFamily = snapshotConfig.getString("hbase.family")
  val snapshotHdfsDir = snapshotConfig.getString("snapshot-dir")
}