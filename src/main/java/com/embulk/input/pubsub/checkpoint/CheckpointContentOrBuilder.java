// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: src/main/resources/checkpoint.proto

package com.embulk.input.pubsub.checkpoint;

public interface CheckpointContentOrBuilder extends
    // @@protoc_insertion_point(interface_extends:CheckpointContent)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>repeated .google.pubsub.v1.PubsubMessage messages = 1;</code>
   */
  java.util.List<com.google.pubsub.v1.PubsubMessage> 
      getMessagesList();
  /**
   * <code>repeated .google.pubsub.v1.PubsubMessage messages = 1;</code>
   */
  com.google.pubsub.v1.PubsubMessage getMessages(int index);
  /**
   * <code>repeated .google.pubsub.v1.PubsubMessage messages = 1;</code>
   */
  int getMessagesCount();
  /**
   * <code>repeated .google.pubsub.v1.PubsubMessage messages = 1;</code>
   */
  java.util.List<? extends com.google.pubsub.v1.PubsubMessageOrBuilder> 
      getMessagesOrBuilderList();
  /**
   * <code>repeated .google.pubsub.v1.PubsubMessage messages = 1;</code>
   */
  com.google.pubsub.v1.PubsubMessageOrBuilder getMessagesOrBuilder(
      int index);
}
