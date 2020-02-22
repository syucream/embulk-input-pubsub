package org.embulk.input.pubsub

import org.embulk.config.{Config, ConfigInject, Task}
import org.embulk.spi.BufferAllocator

trait PluginTask extends Task {

  @Config("project_id")
  def getProjectId: String

  @Config("subscription")
  def getSubscription: String

  @Config("json_keyfile")
  def getJsonKeyfile: String

  @Config("max_messages")
  def getMaxMessages: Long

  @ConfigInject
  def getBufferAllocator: BufferAllocator

  def getCheckpoints: Seq[Checkpoint]
  def setCheckpoints(checkpoints: Seq[Checkpoint]): Unit
}
