package akka.persistence.hbase.journal

import akka.persistence.hbase.{ AsyncBaseUtils, HBaseSerialization, HBaseUtils }
import java.util.{ ArrayList => JArrayList }
import java.{ util => ju }
import org.apache.hadoop.conf.Configuration
import org.hbase.async.{ HBaseClient, KeyValue }

// todo split into one API classes and register the impls as extensions
trait HBaseJournalBase extends HBaseSerialization
    with HBaseUtils with AsyncBaseUtils {

  def client: HBaseClient

  def hBasePersistenceSettings: HBaseJournalConfig
  def hadoopConfig: Configuration

  type AsyncBaseRows = JArrayList[JArrayList[KeyValue]]

}
