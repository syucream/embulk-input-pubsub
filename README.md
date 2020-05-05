# embulk-input-pubsub

[![Gem Version](https://badge.fury.io/rb/embulk-input-pubsub.svg)](https://badge.fury.io/rb/embulk-input-pubsub)

[Google Cloud Pub/Sub](https://cloud.google.com/pubsub?hl=en) input plugin for Embulk. 

## Overview

* **Plugin type**: input
* **Guess supported**: no

## Configuration

- **project_id**: GCP project_id (string, required)
- **subscription_id**: Pub/Sub subscription name (string, required)
- **json_keyfile**: A path to GCP credential json file (string, required)
- **max_messages**: A max number of messages on once pubsub call (integer, optional)
- **checkpoint_basedir**: A path to checkpoint dir (string, optional)
- **checkpoint**: A path to checkpoint file (string, optional)

### Checkpoint

Goocle Cloud Pub/Sub removes stored messages by ack calls or expiration.
So `embulk-input-pubsub` ensures to recovery data-loss with checkpoints which's a fashion used in Apache Flink / Apache Beam.
It 1) pulls messages from Pub/Sub, 2) preserves a checkpoint which contains the messages and 3) ack to pubsub.
If you got failures on Embulk tasks, you can embulk-resume with the checkpoints. And also you can do simply `embulk-run` with `checkpoint`.

If you want checkpointing, you need to set `checkpoint_basedir` to preserve checkpoint files on local filesystem. if none, it uses on-memory store.
If you want to recover state from checkpoint, you need to set `checkpoint`. It restores transaction states from given checkpoint instead of pulling message from pubsub.

The checkpoint is implemented as a Protocol Buffers message.

## Example

- pubsub -> stdout config example

```yaml 
in:
  type: pubsub
  project_id: <your-project-id>
  subscription_id: <your-subscription-name>
  json_keyfile: /path/to/credential.json
  max_messages: 100
  checkpoint_basedir: /tmp/embulk-input-pubsub/

out:
  type: stdout
```

You execute the example, then you'll get the result:

```
 $ embulk run examples/pubsub2stdout.yaml
2020-05-06 00:44:05.093 +0900: Embulk v0.9.23
2020-05-06 00:44:06.540 +0900 [WARN] (main): DEPRECATION: JRuby org.jruby.embed.ScriptingContainer is directly injected.
2020-05-06 00:44:10.743 +0900 [INFO] (main): Gem's home and path are set by default: "/Users/ryo/.embulk/lib/gems"
2020-05-06 00:44:12.551 +0900 [INFO] (main): Started Embulk v0.9.23
2020-05-06 00:44:12.858 +0900 [INFO] (0001:transaction): Loaded plugin embulk-input-pubsub (0.0.1)
2020-05-06 00:44:18.332 +0900 [INFO] (0001:transaction): Created a new checkpoint! : /tmp/embulk-input-pubsub/checkpoint--1576110815
2020-05-06 00:44:18.336 +0900 [INFO] (0001:transaction): Using local thread executor with max_threads=8 / output tasks 4 = input tasks 1 * 4
2020-05-06 00:44:18.354 +0900 [INFO] (0001:transaction): {done:  0 / 1, running: 0}
aaa,{}
2020-05-06 00:44:18.428 +0900 [INFO] (0001:transaction): {done:  1 / 1, running: 0}
2020-05-06 00:44:18.436 +0900 [INFO] (main): Committed.
2020-05-06 00:44:18.436 +0900 [INFO] (main): Next config diff: {"in":{},"out":{}}
```

## Development

```shell script
$ ./gradlew gem
```

## TODO

- Change it to a FileInputPlugin to be applicable for parser plugins
- Remote filesystem based checkpointing
