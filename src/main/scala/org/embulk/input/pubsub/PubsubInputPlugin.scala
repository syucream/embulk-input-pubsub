package org.embulk.input.pubsub

import java.nio.charset.StandardCharsets
import java.util.{Base64, Optional, List => JList}

import com.fasterxml.jackson.databind.ObjectMapper
import org.embulk.config.{ConfigDiff, ConfigException, ConfigSource, TaskReport, TaskSource}
import org.embulk.input.pubsub.checkpoint.StoredCheckpoint
import org.embulk.spi.`type`.Types
import org.embulk.spi.{DataException, Exec, InputPlugin, PageBuilder, PageOutput, Schema}
import org.embulk.spi.json.JsonParser
import org.slf4j.LoggerFactory

import scala.jdk.OptionConverters._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

case class PubsubInputPlugin() extends InputPlugin {
  private val logger = LoggerFactory.getLogger(this.getClass)

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

    if (!task.getCheckpoint.isPresent) {
      val sub = PubsubBatchSubscriber.of(task)
      val checkpoint = sub.pull(task.getMaxMessages, task.getCheckpointBasedir.toScala).get
      task.setCheckpoint(Optional.of(checkpoint.id))

      logger.info(s"Created a new checkpoint! : ${checkpoint.id}")
    }

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
    val task = taskSource.loadTask(classOf[PluginTask])

    val checkpointId = task.getCheckpoint.get()
    val checkpoint = StoredCheckpoint.from(checkpointId, task.getCheckpointBasedir.isPresent)
    checkpoint match {
      case Success(sc) =>
        sc.cleanup match {
          case Success(_) =>
          case Failure(e) => logger.error(s"failed to cleanup: ${e.toString}")
        }
      case Failure(e) => logger.error(s"failed to fetch checkpoint: ${e.toString}")
    }
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

    val checkpointId = task.getCheckpoint.get()
    val checkpoint = StoredCheckpoint.from(checkpointId, task.getCheckpointBasedir.isPresent)
    val messages = checkpoint match {
      case Success(cp) => cp.content.getMessagesList.asScala
      case _ => throw new DataException(s"unexpected checkpoint state: ${checkpoint}")
    }

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
