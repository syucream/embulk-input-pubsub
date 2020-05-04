Embulk::JavaPlugin.register_input(
  "pubsub", "org.embulk.input.pubsub.PubsubInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
