package org.embulk.input.pubsub

import com.google.pubsub.v1.PubsubMessage

case class Checkpoint private (messages: Seq[PubsubMessage])

object Checkpoint {
  def withoutPersistency(messages: Seq[PubsubMessage]): Checkpoint =
    Checkpoint(messages)
}
