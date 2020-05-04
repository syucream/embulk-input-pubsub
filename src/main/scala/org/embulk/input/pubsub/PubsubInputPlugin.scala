package org.embulk.input.pubsub

import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.{List => JList}

import com.fasterxml.jackson.databind.ObjectMapper
import org.embulk.config.{ConfigDiff, ConfigException, ConfigSource, TaskReport, TaskSource}
import org.embulk.spi.`type`.Types
import org.embulk.spi.PageBuilder
import org.embulk.spi.json.JsonParser
import org.embulk.spi.{Exec, InputPlugin, PageOutput, Schema}

case class PubsubInputPlugin() extends InputPlugin {
  private val jsonParser = new JsonParser()
  private val objectMapper = new ObjectMapper()

  private val schema = Schema
    .builder()
    .add("payload", Types.STRING) // string or base64 encoded bytes
    .add("attribute", Types.JSON)
    .build()

  override def transaction(
      config: ConfigSource,
      control: InputPlugin.Control
  ): ConfigDiff = {
    val task = config.loadConfig(classOf[PluginTask])

    // TODO pull pubsub messages and preserve checkpoint here

    resume(task.dump(), schema, task.getNumTasks, control)
  }

  override def resume(
      taskSource: TaskSource,
      schema: Schema,
      taskCount: Int,
      control: InputPlugin.Control
  ): ConfigDiff = {
    control.run(taskSource, schema, taskCount)
    Exec.newConfigDiff()
  }

  override def cleanup(
      taskSource: TaskSource,
      schema: Schema,
      taskCount: Int,
      successTaskReports: JList[TaskReport]
  ): Unit = {
    // nothing to do
  }

  override def run(
      taskSource: TaskSource,
      schema: Schema,
      taskIndex: Int,
      output: PageOutput
  ): TaskReport = {
    val task = taskSource.loadTask(classOf[PluginTask])
    val allocator = task.getBufferAllocator
    val pageBuilder = new PageBuilder(allocator, schema, output)

    val encoder = task.getPayloadEncoding match {
      case "string" => (data: Array[Byte]) => new String(data, StandardCharsets.UTF_8)
      case "binary" => (data: Array[Byte]) => Base64.getEncoder.encodeToString(data)
      case e => throw new ConfigException(s"unsupported encoding: ${e}")
    }

    val sub = PubsubBatchSubscriber.of(task)
    val checkpoint = sub.pull(task.getMaxMessages).get
    val messages = checkpoint.messages

    messages.foreach { msg =>
      pageBuilder.setString(
        pageBuilder.getSchema.getColumn(0),
        encoder(msg.getData.toByteArray)
      )

      val json = objectMapper.writeValueAsString(msg.getAttributesMap)
      pageBuilder.setJson(
        pageBuilder.getSchema.getColumn(1),
        jsonParser.parse(json)
      )

      pageBuilder.addRecord()
    }
    pageBuilder.finish()

    Exec.newTaskReport()
  }

  override def guess(config: ConfigSource): ConfigDiff =
    Exec.newConfigDiff()

}
