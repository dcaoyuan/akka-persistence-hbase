package akka.persistence.hbase.snapshot

import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{ SelectedSnapshot, SnapshotSelectionCriteria }
import akka.persistence.SnapshotMetadata
import akka.actor.ActorLogging
import scala.concurrent.Future

class HadoopSnapshotStore extends SnapshotStore with ActorLogging {

  val snap = HadoopSnapshotterExtension(context.system)

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] =
    snap.loadAsync(persistenceId, criteria)

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] =
    snap.saveAsync(metadata, snapshot)

  def saved(metadata: SnapshotMetadata): Unit =
    snap.saved(metadata)

  def deleteAsync(metadata: SnapshotMetadata): Future[Unit] =
    snap.deleteAsync(metadata)

  def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] =
    snap.deleteAsync(persistenceId, criteria)
}

