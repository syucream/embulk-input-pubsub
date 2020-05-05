package org.embulk.input.pubsub

import java.io.FileInputStream

import com.google.api.gax.core.FixedCredentialsProvider
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStubSettings}
import com.google.protobuf.Empty
import com.google.pubsub.v1.{AcknowledgeRequest, ProjectSubscriptionName, PullRequest, PullResponse}
import org.embulk.input.pubsub.checkpoint.StoredCheckpoint

import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}

/**
 * A subscriber for Cloud Pub/Sub calls batch based pulls with checkpoint.
 *
 * @param projectId
 * @param subscriptionName
 * @param pathToCredJson
 */
case class PubsubBatchSubscriber private (projectId: String, subscriptionName: String, pathToCredJson: String) {
  private val credentials = GoogleCredentials.fromStream(new FileInputStream(pathToCredJson))
  private val settings = SubscriberStubSettings.newBuilder()
    .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
    .setTransportChannelProvider(SubscriberStubSettings.defaultGrpcTransportProviderBuilder().build())
    .build()

  def pull(count: Int, checkpointDir: Option[String]): Try[StoredCheckpoint] = {
    val subscription = ProjectSubscriptionName.of(projectId, subscriptionName).toString
    val subscriber = GrpcSubscriberStub.create(settings)

    for {
      res <- pullImpl(subscriber, subscription, count)
      messages = res.getReceivedMessagesList.asScala
      checkpoint <- StoredCheckpoint.create(messages.map(_.getMessage).toSeq, checkpointDir)
      _ <- ackImpl(subscriber, subscription, messages.map(_.getAckId))
    } yield checkpoint
  }

  private def pullImpl(subscriber: GrpcSubscriberStub, subscription: String, count: Int): Try[PullResponse] = {
    val req = PullRequest.newBuilder()
      .setSubscription(subscription)
      .setReturnImmediately(true)
      .setMaxMessages(count)
      .build()

    Try(subscriber.pullCallable().call(req))
  }

  private def ackImpl(subscriber: GrpcSubscriberStub, subscription: String, ackIds: Iterable[String]): Try[Empty] = {
    if (ackIds.nonEmpty) {
      val ack = AcknowledgeRequest.newBuilder()
        .setSubscription(subscription)
        .addAllAckIds(ackIds.asJava)
        .build()

      Try(subscriber.acknowledgeCallable().call(ack))
    } else {
      Success(Empty.getDefaultInstance)
    }
  }

}

object PubsubBatchSubscriber {
  def of(task: PluginTask): PubsubBatchSubscriber =
    PubsubBatchSubscriber(task.getProjectId, task.getSubscriptionId, task.getJsonKeyfile)
}
