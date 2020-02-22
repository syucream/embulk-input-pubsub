package org.embulk.input.pubsub

import java.io.FileInputStream
import java.time.{Duration, Instant}

import com.google.api.gax.core.FixedCredentialsProvider
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Subscriber}
import com.google.pubsub.v1.{ProjectSubscriptionName, PubsubMessage}

import scala.collection.mutable

case class PubsubBatchSubscriber private (projectId: String, subscriptionName: String, pathToCredJson: String) {
  private val serviceAccount = new FileInputStream(pathToCredJson)
  private val credentials = GoogleCredentials.fromStream(serviceAccount)

  private val receiver = BoundedMessageReceiver()
  private val sub = Subscriber
    .newBuilder(ProjectSubscriptionName.of(projectId, subscriptionName), receiver)
    .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
    .build()

  def pull(count: Long, timeout: Duration = Duration.ofSeconds(30L)): Seq[Checkpoint] = {
    sub.startAsync().awaitRunning()

    val startedAt = Instant.now()
    while(receiver.count >= count || Instant.now.isAfter(startedAt.plus(timeout))) {
      Thread.sleep(1000L)
    }

    sub.stopAsync()
    sub.awaitRunning()

    receiver.checkpoints.toSeq
  }

  private case class BoundedMessageReceiver() extends MessageReceiver {
    // TODO save to disk
    val checkpoints = mutable.Seq.empty[Checkpoint]

    // metrics
    var count: Long = 0L

    override def receiveMessage(message: PubsubMessage, consumer: AckReplyConsumer): Unit = {
      checkpoints.appended(Checkpoint.withoutPersistency(message))
      consumer.ack()
      count = count + 1
    }
  }
}

object PubsubBatchSubscriber {
  def of(task: PluginTask): PubsubBatchSubscriber =
    PubsubBatchSubscriber(task.getProjectId, task.getSubscription, task.getJsonKeyfile)
}
