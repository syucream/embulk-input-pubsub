package org.embulk.input.pubsub

import com.google.pubsub.v1.PubsubMessage

case class Checkpoint private (message: PubsubMessage)

object Checkpoint {
  def withoutPersistency(message: PubsubMessage): Checkpoint =
    Checkpoint(message)
}
