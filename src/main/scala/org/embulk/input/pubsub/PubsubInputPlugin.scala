package org.embulk.input.pubsub

import java.util.Base64
import java.util.{List => JList}

import com.fasterxml.jackson.databind.ObjectMapper
import org.embulk.config.{ConfigDiff, ConfigSource, TaskReport, TaskSource}
import org.embulk.spi.`type`.Types
import org.embulk.spi.PageBuilder
import org.embulk.spi.json.JsonParser
import org.embulk.spi.{Exec, InputPlugin, PageOutput, Schema}

import scala.jdk.CollectionConverters._

case class PubsubInputPlugin() extends InputPlugin {
  private val base64Encoder = Base64.getEncoder
  private val jsonParser = new JsonParser()
  private val objectMapper = new ObjectMapper()

  private val schema = Schema.builder()
    .add("payload", Types.JSON) // valid???
    .add("attribute", Types.JSON)
    .build()

  override def transaction(
      config: ConfigSource,
      control: InputPlugin.Control
  ): ConfigDiff = {
    val task = config.loadConfig(classOf[PluginTask])

    val sub = PubsubBatchSubscriber.of(task)
    task.setCheckpoints(sub.pull(task.getMaxMessages).asJava)

    resume(task.dump(), schema, 1, control)
  }

  override def resume(
      taskSource: TaskSource,
      schema: Schema,
      taskCount: Int,
      control: InputPlugin.Control
  ): ConfigDiff = {
    // TODO support fault case
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

    task.getCheckpoints.asScala.foreach { c =>
      pageBuilder.setString(
        pageBuilder.getSchema.getColumn(0),
        base64Encoder.encode(c.message.getData.toByteArray).toString
      )

      val json = objectMapper.writeValueAsString(c.message.getAttributesMap)
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
