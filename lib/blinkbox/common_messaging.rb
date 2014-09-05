require "bunny"
require "uri"
require "active_support/core_ext/hash/keys"
require "active_support/core_ext/hash/deep_merge"
require "active_support/core_ext/string/inflections"
require "ruby_units"
require "forwardable"
require "json-schema"
require "securerandom"
require "logger"

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
        user: "guest",
        pass: "guest",
        vhost: "",
        log_level: Logger::WARN,
        automatically_recover: true,
        threaded: true,
        continuation_timeout: 4000
      },
      retry_interval: {
        initial: Unit("5 seconds"),
        max: Unit("5 seconds")
      },
      logger: Logger.new(nil)
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
      @@config = DEFAULT_CONFIG

      unless config[:url].nil?
        uri = URI.parse(config[:url])
        @@config.deep_merge!({
          bunny: {
            host: uri.host,
            port: uri.port,
            user: uri.user,
            pass: uri.password,
            vhost: uri.path
          }
        })
      end

      %i{initialRetryInterval maxRetryInterval}.each do |unit_key|
        if config[unit_key]
          config[unit_key] = Unit(config[unit_key]) unless config[unit_key].is_a?(Unit)

          @@config.deep_merge!({
            retry_interval: {
              unit_key.to_s.sub('RetryInterval','').to_sym => config[unit_key]
            }
          })
        end
      end

      self.logger = logger unless logger.nil?
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
      %i{debug info warn error fatal level= level}.each do |m|
        raise ArgumentError, "The logger did not respond to '#{m}'" unless logger.respond_to?(m)
      end
      @@config[:logger] = logger
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
      def_delegators :@queue, :status

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
        @logger = CommonMessaging.config[:logger]
        # We create one channel per queue because it means that any issues are isolated
        # and we can start a new channel and resume efforts in a segregated manner.
        @channel = connection.create_channel
        @queue = @channel.queue(
          queue_name,
          durable: true,
          auto_delete: false,
          exclusive: false
        )
        @exchange = @channel.headers(
          exchange,
          durable: true,
          auto_delete: false
        )
        Kernel.warn "No bindings were given, the queue is unlikely to receive any messages" if bindings.empty?
        bindings.each do |binding|
          @queue.bind(@exchange, arguments: binding)
        end
      end

      def subscribe(options = {}, &block)
        @queue.subscribe(
          block: options[:block] || true,
          manual_ack: true
        ) { |delivery_info, metadata, payload|
          begin
            klass = Blinkbox::CommonMessaging.class_from_content_type(metadata[:headers]['content-type'])
            object = klass.new(JSON.parse(payload))
            response = yield metadata, object
            case response
            when :ack, true
              @channel.ack(delivery_info[:delivery_tag])
            when :reject, false
              @channel.reject(delivery_info[:delivery_tag], false)
            when :retry
              @channel.reject(delivery_info[:delivery_tag], true)
            else
              raise "Unknown response from subscribe block: #{response}"
            end
          rescue Exception => e
            @logger.error e
            @channel.reject(delivery_info[:delivery_tag])
          end
        }
      end
    end

    class Exchange
      extend Forwardable
      def_delegators :@exchange, :on_return

      # A wrapped class for Bunny::Exchange. Wrapped so we can take care of message validation and header 
      # conventions in the blinkbox Books format.
      #
      # @param [String] exchange_name The name of the Exchange to connect to.
      # @param [String] facility The name of the app or service (we've adopted the GELF naming term across ruby)
      # @param [String] facility_version The version of the app or service which sent the message.
      def initialize(exchange_name, facility: File.basename($0,'.rb'), facility_version: "0.0.0-unknown")
        @app_id = "#{facility}:v#{facility_version}"
        connection = CommonMessaging.connection
        channel = connection.create_channel
        @exchange = channel.headers(
          exchange_name,
          durable: true,
          auto_delete: false
        )
      end

      # Publishes a message to the exchange with blinkbox Books default message headers and properties.
      #
      # Worth noting that because of a quirk of the RabbitMQ Headers Exchange you cannot route on properties
      # so, in order to facilitate routing on content-type, that key is written to the headers by default as
      # well as to the properties.
      #
      # @param [Blinkbox::CommonMessaging::JsonSchemaPowered] data The information which will be sent as the payload of the message. An instance of any class generated by Blinkbox::CommonMessaging.init_from_schema_at.
      # @param [Hash] headers A hash of string keys and string values which will be sent as headers with the message. Used for matching.
      # @param [String] message_id_chain Optional. The message_id_chain of the message which was received in order to prompt this one.
      # @param [Boolean] mandatory Defines whether the message will be returned if it was not routed.
      # block called after publish requiest, before publish certain
      # @return [String] The correlation_id of the message which was delivered.
      def publish(data, headers: {}, message_id_chain: nil, mandatory: true, &block)
        raise ArgumentError, "All published messages must be validated. Please see Blinkbox::CommonMessaging.init_from_schema_at for details." unless data.class.included_modules.include?(JsonSchemaPowered)

        message_id = generate_message_id
        message_id_chain = (message_id_chain || []) << message_id
        correlation_id = message_id_chain.first

        @exchange.publish(
          data.to_json,
          persistent: true,
          mandatory: mandatory,
          content_type: data.content_type,
          correlation_id: correlation_id,
          message_id: message_id,
          app_id: @app_id,
          timestamp: Time.now.to_i,
          headers: {
            "content-type" => data.content_type,
            "message_id_chain" => message_id_chain
          }.merge(headers)
        )

        block.call(message_id) if block_given?

        correlation_id
      end

      private

      def generate_message_id
        SecureRandom.hex(8) # 8 generates a 16 byte string
      end
    end

    module JsonSchemaPowered
      extend Forwardable
      def_delegators :@data, :responds_to?, :to_json, :[]

      def method_missing(m, *args, &block)
        @data.send(m, *args, &block)
      end

      def to_hash
        @data
      end

      def to_s
        classification_string = @data["classification"].map { |cl| 
          "#{cl["realm"]}:#{cl["id"]}"
        }.join(", ")
        "<#{self.class.name.split("::").last}: #{classification_string}>"
      end
    end

    # Generates ruby classes representing blinkbox Books messages from the schema files at the
    # given path.
    #
    # @example Initialising CommonMessaging for sending
    #   Blinkbox::CommonMessaging.init_from_schema_at("ingestion.book.metatdata.v2.schema.json")
    #   msg = Blinkbox::CommonMessaging::IngestionBookMetadataV2.new(title: "A title")
    #   exchange.publish(msg)
    #
    # @example Using the root path
    #   Blinkbox::CommonMessaging.init_from_schema_at("./schema/ingestion/book/metatdata/v2.schema.json")
    #   # => [Blinkbox::CommonMessaging::SchemaIngestionBookMetadataV2]
    #
    #   Blinkbox::CommonMessaging.init_from_schema_at("./schema/ingestion/book/metatdata/v2.schema.json", "./schema")
    #   # => [Blinkbox::CommonMessaging::IngestionBookMetadataV2]
    # 
    # @params [String] path The path to a (or a folder of) json-schema file(s) in the blinkbox Books format.
    # @params [String] root The root path from which namespaces will be calculated. 
    # @return Array of class names generated
    def self.init_from_schema_at(path, root = path)
      raise RuntimeError, "The path #{path} does not exist" unless File.exists?(path)
      return Dir[File.join(path, "**/*.schema.json")].map { |file| init_from_schema_at(file, root) }.flatten if File.directory?(path)

      root = File.dirname(root) if root =~ /\.schema\.json$/
      schema_name = path.sub(%r{^(?:\./)?#{root}/?(.+)\.schema\.json$},"\\1").tr("/",".")
      class_name = class_name_from_schema_name(schema_name)

      # We will re-declare these classes if required, rather than raise an error.
      remove_const(class_name) if constants.include?(class_name.to_sym)

      const_set(class_name, Class.new {
        include JsonSchemaPowered

        def initialize(data = {})
          @data = data.stringify_keys
          JSON::Validator.validate!(self.class.const_get("SCHEMA_FILE"), @data, insert_defaults: true)
        end

        def content_type
          self.class.const_get("CONTENT_TYPE")
        end
      })

      klass = const_get(class_name)
      klass.const_set('CONTENT_TYPE', "application/vnd.blinkbox.books.#{schema_name}+json")
      klass.const_set('SCHEMA_FILE', path)
      klass
    end

    def self.class_from_content_type(content_type)
      raise "No content type was given" if content_type.nil? || content_type.empty?
      begin
        schema_name = content_type.sub(%r{^application/vnd\.blinkbox\.books\.(.+)\+json$}, '\1')
        const_get(class_name_from_schema_name(schema_name))
      rescue
        raise "The schema for the #{content_type} content type has not been loaded"
      end
    end

    def self.class_name_from_schema_name(schema_name)
      schema_name.tr("./", "_").camelcase
    end
  end
end