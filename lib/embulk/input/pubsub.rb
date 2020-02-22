Embulk::JavaPlugin.register_input(
  "firestore", "org.embulk.input.pubsub.PubsubInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
