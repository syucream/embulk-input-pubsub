package org.embulk.input.pubsub

import java.util.Optional

import org.embulk.config.{Config, ConfigDefault, ConfigInject, Task}
import org.embulk.spi.BufferAllocator

trait PluginTask extends Task {

  @Config("project_id")
  def getProjectId: String

  @Config("subscription_id")
  def getSubscriptionId: String

  @Config("json_keyfile")
  def getJsonKeyfile: String

  @Config("num_tasks")
  @ConfigDefault("1")
  def getNumTasks: Int

  @Config("max_messages")
  @ConfigDefault("10")
  def getMaxMessages: Int

  @Config("checkpoint_basedir")
  @ConfigDefault("null")
  def getCheckpointBasedir: Optional[String]

  @Config("checkpoint")
  @ConfigDefault("null")
  def getCheckpoint: Optional[String]
  def setCheckpoint(checkpoint: Optional[String]): Unit

  @Config("payload_encoding")
  @ConfigDefault("\"string\"")
  def getPayloadEncoding: String

  @ConfigInject
  def getBufferAllocator: BufferAllocator
}
