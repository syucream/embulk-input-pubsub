package org.embulk.input.pubsub.checkpoint

import java.io.{FileInputStream, FileOutputStream}

import com.embulk.input.pubsub.checkpoint.CheckpointContent
import com.google.pubsub.v1.PubsubMessage

import scala.collection.mutable
import scala.util.{Success, Try}
import scala.jdk.CollectionConverters._

sealed trait Checkpoint {
  def id: String
  def content: CheckpointContent
}

case class MemoryCheckpoint private (id: String, content: CheckpointContent)
    extends Checkpoint

object MemoryCheckpoint {
  private val storage = mutable.Map[String, MemoryCheckpoint]()

  def from(key: String): Try[MemoryCheckpoint] = Try(storage(key))

  def withoutPersistency(content: CheckpointContent): Checkpoint = {
    val id = content.hashCode().toString
    val checkpoint = MemoryCheckpoint(id, content)
    storage.put(id, checkpoint)
    checkpoint
  }
}

case class LocalFileCheckpoint private (id: String, content: CheckpointContent)
    extends Checkpoint

object LocalFileCheckpoint {
  def from(path: String): Try[LocalFileCheckpoint] = {
    for {
      in <- Try(new FileInputStream(path))
      c <- Try(CheckpointContent.parseFrom(in))
    } yield LocalFileCheckpoint(path, c)
  }

  def withPersistency(
      prefix: String,
      messages: Seq[PubsubMessage]
  ): Try[LocalFileCheckpoint] = {
    val path = s"${prefix}checkpoint-${messages.hashCode().toString}"
    val content = CheckpointContent
      .newBuilder()
      .addAllMessages(messages.asJava)
      .build()

    for {
      out <- Try(new FileOutputStream(path))
      _ <- Try(content.writeTo(out))
      _ <- Try(out.close())
    } yield LocalFileCheckpoint(path, content)
  }
}

object Checkpoint {
  def create(
      messages: Seq[PubsubMessage],
      dir: Option[String]
  ): Try[Checkpoint] = {
    dir match {
      case Some(d) =>
        LocalFileCheckpoint.withPersistency(d, messages)
      case _ =>
        val content = CheckpointContent
          .newBuilder()
          .addAllMessages(messages.asJava)
          .build()
        Success(MemoryCheckpoint.withoutPersistency(content))
    }
  }

  def from(id: String, persistent: Boolean): Try[Checkpoint] = {
    if (persistent) {
      LocalFileCheckpoint.from(id)
    } else {
      MemoryCheckpoint.from(id)
    }
  }
}
