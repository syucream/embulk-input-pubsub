package org.embulk.input.pubsub.checkpoint

import com.google.pubsub.v1.PubsubMessage

sealed trait Checkpoint {
  def messages: Seq[PubsubMessage]
}

case class MemoryCheckpoint private (messages: Seq[PubsubMessage]) extends Checkpoint

object Checkpoint {
  def withoutPersistency(messages: Seq[PubsubMessage]): Checkpoint =
    MemoryCheckpoint(messages)
}
