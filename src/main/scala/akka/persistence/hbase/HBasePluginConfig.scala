package akka.persistence.hbase

import akka.persistence.hbase.journal.HBaseJournalInit
import com.typesafe.config.Config
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.duration._

case class StorePathPasswordConfig(path: String, password: String)

class HBasePluginConfig(config: Config) {
  import HBasePluginConfig._

  val journalConfig = config.getConfig("hbase-journal")
  val snapshotConfig = config.getConfig("hadoop-snapshot-store")

  val zookeeperQuorum = journalConfig.getString("hadoop-pass-through.hbase.zookeeper.quorum")
  val zookeeperParent = journalConfig.getString("hadoop-pass-through.zookeeper.znode.parent")
  val maxMessageBatchSize = config.getInt("max-message-batch-size")
  val deleteRetries: Int = config.getInt("delete-retries")
  val writeRetries: Int = config.getInt("write-retries")

  val publishTestingEvents = journalConfig.getBoolean("publish-testing-events")
  val scanBatchSize = journalConfig.getInt("scan-batch-size")

  val hadoopConfiguration = if (config ne null) HBaseJournalInit.getHBaseConfig(config) else null

  val connectionRetries: Int = config.getInt("connect-retries")
  val connectionRetryDelay: FiniteDuration = config.getDuration("connect-retry-delay", TimeUnit.MILLISECONDS).millis

}

object HBasePluginConfig {

  val keyspaceNameRegex = """^("[a-zA-Z]{1}[\w]{0,47}"|[a-zA-Z]{1}[\w]{0,47})$"""

  /**
   * Builds replication strategy command to create a keyspace.
   */
  def getReplicationStrategy(strategy: String, replicationFactor: Int, dataCenterReplicationFactors: Seq[String]): String = {

    def getDataCenterReplicationFactorList(dcrfList: Seq[String]): String = {
      val result: Seq[String] = dcrfList match {
        case null | Nil => throw new IllegalArgumentException("data-center-replication-factors cannot be empty when using NetworkTopologyStrategy.")
        case dcrfs => dcrfs.map {
          dataCenterWithReplicationFactor =>
            dataCenterWithReplicationFactor.split(":") match {
              case Array(dataCenter, replicationFactor) => s"'$dataCenter':$replicationFactor"
              case msg                                  => throw new IllegalArgumentException(s"A data-center-replication-factor must have the form [dataCenterName:replicationFactor] but was: $msg.")
            }
        }
      }
      result.mkString(",")
    }

    strategy.toLowerCase() match {
      case "simplestrategy"          => s"'SimpleStrategy','replication_factor':$replicationFactor"
      case "networktopologystrategy" => s"'NetworkTopologyStrategy',${getDataCenterReplicationFactorList(dataCenterReplicationFactors)}"
      case unknownStrategy           => throw new IllegalArgumentException(s"$unknownStrategy as replication strategy is unknown and not supported.")
    }
  }

  /**
   * Validates that the supplied keyspace name is valid based on docs found here:
   *   http://docs.datastax.com/en/cql/3.0/cql/cql_reference/create_keyspace_r.html
   * @param keyspaceName - the keyspace name to validate.
   * @return - String if the keyspace name is valid, throws IllegalArgumentException otherwise.
   */
  def validateKeyspaceName(keyspaceName: String): String = keyspaceName.matches(keyspaceNameRegex) match {
    case true  => keyspaceName
    case false => throw new IllegalArgumentException(s"Invalid keyspace name. A keyspace may 32 or fewer alpha-numeric characters and underscores. Value was: $keyspaceName")
  }

  /**
   * Validates that the supplied table name meets Cassandra's table name requirements.
   * According to docs here: https://cassandra.apache.org/doc/cql3/CQL.html#createTableStmt :
   *
   * @param tableName - the table name to validate
   * @return - String if the tableName is valid, throws an IllegalArgumentException otherwise.
   */
  def validateTableName(tableName: String): String = tableName.matches(keyspaceNameRegex) match {
    case true  => tableName
    case false => throw new IllegalArgumentException(s"Invalid table name. A table name may 32 or fewer alpha-numeric characters and underscores. Value was: $tableName")
  }
}