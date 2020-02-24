package org.embulk.input.pubsub

import java.util.{List => JList}

import org.embulk.config.{Config, ConfigInject, Task}
import org.embulk.spi.BufferAllocator

trait PluginTask extends Task {

  @Config("project_id")
  def getProjectId: String

  @Config("subscription_id")
  def getSubscriptionId: String

  @Config("json_keyfile")
  def getJsonKeyfile: String

  @Config("max_messages")
  def getMaxMessages: Long

  @ConfigInject
  def getBufferAllocator: BufferAllocator

  def getCheckpoints: JList[Checkpoint]
  def setCheckpoints(checkpoints: JList[Checkpoint]): Unit
}
