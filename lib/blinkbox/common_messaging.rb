require "bunny"
require "uri"
require "active_support/core_ext/hash/keys"
require "active_support/core_ext/hash/deep_merge"
require "active_support/core_ext/string/inflections"
require "ruby_units"
require "forwardable"
require "json-schema"

module Blinkbox
  # A group of methods and classes which enable the delivery of messages through the
  # blinkbox Books ecosystem via AMQP.
  #
  # `CommonMessaging.configure` should be used to set up connection details first, then 
  # every subsequent call to `CommonMessaging::Queue.new` will create a `Bunny::Queue` object
  # using the connection details that were present at the time.
  module CommonMessaging
    # The default RabbitMQ connection details, in the format that Bunny needs them.
    DEFAULT_CONFIG = {
      bunny: {
        host: "localhost",
        port: 5672,
        vhost: "",
        log_level: Logger::WARN,
        automatically_recover: true,
        threaded: true,
        continuation_timeout: 4000
      },
      retry_interval: {
        initial: Unit("5 seconds"),
        max: Unit("5 seconds")
      }
    }

    # This method only stores connection details for calls to `CommonMessaging::Queue.new`.
    # Any queues already created will not be affected by subsequent calls to this method.
    #
    # This method converts the given options from the blinkbox Books common config format
    # to the format required for Bunny so that calls like the following are possible:
    #
    # @example Using with CommonConfig
    #   require "blinkbox/common_config"
    #   require "blinkbox/common_messaging"
    #   
    #   config = Blinkbox::CommonConfig.new
    #   Blinkbox::CommonMessaging.configure(config.tree(:rabbitmq))
    #
    # @params [Hash] config The configuration options needed for an MQ connection.
    # @params config [String] :url The URL to the RabbitMQ server, eg. amqp://user:pass@host.name:1234/virtual_host
    # @params config [Unit] :initialRetryInterval The interval at which re-connection attempts should be made when a RabbitMQ failure first occurs.
    # @params config [Unit] :maxRetryInterval The maximum interval at which RabbitMQ reconnection attempts should back off to.
    # params [] logger The logger instance which should be used by Bunny
    def self.configure(config, logger = nil)
      # TODO: retry intervals
      uri = URI.parse(config[:url])
      @@config = DEFAULT_CONFIG.deep_merge({
        bunny: {
          host: uri.host,
          port: uri.port,
          user: uri.user,
          pass: uri.password,
          vhost: uri.path
        },
        retry_interval: {
          initial: config[:initialRetryInterval],
          max: config[:maxRetryInterval]
        }
      })
    end

    # Returns the current config being used (as used by Bunny)
    #
    # @return [Hash]
    def self.config
      @@config rescue DEFAULT_CONFIG
    end

    # Sets the logger delivered to Bunny when new connections are made
    #
    # @param [] logger The object to which log messages should be sent.
    def self.logger=(logger)
      # TODO
    end

    # Returns (and starts if necessary) the connection to the RabbitMQ server as specified by the current
    # config. Will keep only one connection per configuration at any time and will return or create a new connection
    # as necessary.
    #
    # Application code should not need to use this method.
    #
    # @return [Bunny::Session]
    def self.connection
      @@connections ||= {}
      @@connections[config] ||= Bunny.new(config[:bunny])
      @@connections[config].start
      @@connections[config]
    end

    # A proxy class for generating queues and binding them to exchanges using Bunny. In the
    # format expected from blinkbox Books services.
    class Queue
      extend Forwardable
      def_delegators :@queue, :subscribe, :status

      # Create a queue object for subscribing to messages with.
      #
      # NB. There is no way to know what bindings have already been made for a queue, so all code
      # subscribing to a queue should cope with receiving messages it's not expecting.
      #
      # @params [String] queue_name The name of the queue which should be used and (if necessary) created.
      # @params [String] exchange The name of the Exchange to bind to. The default value should be avoided for production uses.
      # @params [Array,Hash] bindings An array of hashes, each on detailing the parameters for a new binding.
      # @return [Bunny::Queue] A blinkbox managed Bunny Queue object
      def initialize(queue_name, exchange: "amq.headers", bindings: [])
        connection = CommonMessaging.connection
        # We create one channel per queue because it means that any issues are isolated
        # and we can start a new channel and resume efforts in a segregated manner.
        channel = connection.create_channel
        @queue = channel.queue(
          queue_name,
          durable: true,
          auto_delete: false,
          exclusive: false
        )
        @exchange = channel.headers(
          exchange,
          durable: true,
          auto_delete: false
        )
        bindings.each do |binding|
          @queue.bind(@exchange, arguments: binding)
        end
      end
    end

    class Exchange

    end

    module JsonSchemaPowered
      extend Forwardable
      def_delegators :@data, :responds_to?, :to_json

      def method_missing(m, *args, &block)
        @data.send(m, *args, &block)
      end
    end

    # Generates ruby classes representing blinkbox Books messages from the schema files at the
    # given path.
    #
    # @example Initialising CommonMessaging for sending
    #   Blinkbox::CommonMessaging.init_from_schema_at("./schema/ingestion.book.metatdata.v2.schema.json")
    #   msg = Blinkbox::CommonMessaging::IngestionBookMetadataV2.new(title: "A title")
    #   exchange.publish(msg)
    # 
    # @params [String] path The path to a (or a folder of) json-schema file(s) in the blinkbox Books format.
    # @return Array of class names generated
    def self.init_from_schema_at(path)
      raise RuntimeError, "The path #{path} does not exist" unless File.exists?(path)
      return Dir[File.join(path,"*.schema.json")].map { |file| init_from_schema_at(file) }.flatten if File.directory?(path)

      schema_name = File.basename(path, ".schema.json")
      class_name = schema_name.tr(".", "_").camelcase

      remove_const(class_name) if constants.include?(class_name.to_sym)

      const_set(class_name, Class.new {
        include JsonSchemaPowered
        attr_reader :content_type
        @@schema_file = path

        def initialize(data)
          @data = data.stringify_keys
          JSON::Validator.validate!(@@schema_file, @data, insert_defaults: true)
          @content_type = "application/vnd.blinkbox.books." << File.basename(@@schema_file,".schema.json")
        end
      })
    end
  end
end