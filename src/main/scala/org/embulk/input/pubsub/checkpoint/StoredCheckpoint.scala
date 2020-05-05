package org.embulk.input.pubsub.checkpoint

import java.io.{File, FileInputStream, FileOutputStream}

import com.embulk.input.pubsub.checkpoint.Checkpoint
import com.google.pubsub.v1.PubsubMessage
import org.embulk.config.ConfigException

import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import scala.jdk.CollectionConverters._

/**
 * A checkpoint stored in a (maybe)persistent storage.
 */
sealed trait StoredCheckpoint {
  def id: String
  def content: Checkpoint
  def cleanup: Try[Unit]
}

object StoredCheckpoint {
  def create(
              messages: Seq[PubsubMessage],
              dir: Option[String]
            ): Try[StoredCheckpoint] = {
    dir match {
      case Some(d) =>
        LocalFileStoredCheckpoint.withPersistency(d, messages)
      case _ =>
        val content = Checkpoint
          .newBuilder()
          .addAllMessages(messages.asJava)
          .build()
        Success(MemoryStoredStoredCheckpoint.withoutPersistency(content))
    }
  }

  def from(id: String, persistent: Boolean): Try[StoredCheckpoint] = {
    if (persistent) {
      LocalFileStoredCheckpoint.from(id)
    } else {
      MemoryStoredStoredCheckpoint.from(id)
    }
  }
}

/**
 * A checkpoint stored in only memory which doesn't have persistence.
 *
 * @param id
 * @param content
 */
case class MemoryStoredStoredCheckpoint private(id: String, content: Checkpoint)
    extends StoredCheckpoint {
  import MemoryStoredStoredCheckpoint._

  override def cleanup: Try[Unit] = {
    storage.remove(id) match {
      case Some(_) => Success(())
      case _ => Failure(new ConfigException(s"A checkpoint ${id} is not deletable"))
    }
  }
}

object MemoryStoredStoredCheckpoint {
  private val storage = mutable.Map[String, MemoryStoredStoredCheckpoint]()

  def from(key: String): Try[MemoryStoredStoredCheckpoint] = Try(storage(key))

  def withoutPersistency(content: Checkpoint): StoredCheckpoint = {
    val id = content.hashCode().toString
    val checkpoint = MemoryStoredStoredCheckpoint(id, content)
    storage.put(id, checkpoint)
    checkpoint
  }

}

/**
 * A checkpoint stored in local filesystem.
 *
 * @param id
 * @param content
 */
case class LocalFileStoredCheckpoint private(id: String, content: Checkpoint)
    extends StoredCheckpoint {
  override def cleanup: Try[Unit] = {
    for {
      f <- Try(new File(id))
      _ <- Try(f.delete)
    } yield ()
  }
}

object LocalFileStoredCheckpoint {
  def from(path: String): Try[LocalFileStoredCheckpoint] = {
    for {
      in <- Try(new FileInputStream(path))
      c <- Try(Checkpoint.parseFrom(in))
    } yield LocalFileStoredCheckpoint(path, c)
  }

  def withPersistency(
      prefix: String,
      messages: Seq[PubsubMessage]
  ): Try[LocalFileStoredCheckpoint] = {
    val path = s"${prefix}checkpoint-${messages.hashCode().toString}"
    val content = Checkpoint
      .newBuilder()
      .addAllMessages(messages.asJava)
      .build()

    for {
      out <- Try(new FileOutputStream(path))
      _ <- Try(content.writeTo(out))
      _ <- Try(out.close())
    } yield LocalFileStoredCheckpoint(path, content)
  }
}

