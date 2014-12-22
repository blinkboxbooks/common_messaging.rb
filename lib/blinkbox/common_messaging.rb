require "blinkbox/common_messaging/version"
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
require "blinkbox/common_messaging/header_detectors"
require "blinkbox/common_messaging/queue"
require "blinkbox/common_messaging/exchange"

module Blinkbox
  # A group of methods and classes which enable the delivery of messages through the
  # blinkbox Books ecosystem via AMQP.
  #
  # `CommonMessaging.configure!` should be used to set up connection details first, then 
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
        vhost: "/",
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
    #   Blinkbox::CommonMessaging.configure!(config.tree(:rabbitmq))
    #
    # @param [Hash] config The configuration options needed for an MQ connection.
    # @option config [String] :url The URL to the RabbitMQ server, eg. amqp://user:pass@host.name:1234/virtual_host
    # @option config [Unit] :initialRetryInterval The interval at which re-connection attempts should be made when a RabbitMQ failure first occurs.
    # @option config [Unit] :maxRetryInterval The maximum interval at which RabbitMQ reconnection attempts should back off to.
    # @param [#debug, #info, #warn, #error, #fatal] logger The logger instance which should be used by Bunny
    def self.configure!(config, logger = nil)
      @@config = DEFAULT_CONFIG

      unless config[:url].nil?
        uri = URI.parse(config[:url])
        @@config.deep_merge!(
          bunny: {
            host: uri.host,
            port: uri.port,
            user: uri.user,
            pass: uri.password,
            vhost: uri.path
          }
        )
      end

      %i{initialRetryInterval maxRetryInterval}.each do |unit_key|
        if config[unit_key]
          config[unit_key] = Unit(config[unit_key]) unless config[unit_key].is_a?(Unit)

          @@config.deep_merge!(
            retry_interval: {
              unit_key.to_s.sub('RetryInterval', '').to_sym => config[unit_key]
            }
          )
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
      @@config[:bunny][:logger] = logger
    end

    # Returns (and starts if necessary) the connection to the RabbitMQ server as specified by the current
    # config. Will keep only one connection per configuration at any time and will return or create a new connection
    # as necessary. Channels are created with publisher confirmations.
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

    # Blocks until all the open connections have been closed, calling the block with any message_ids which haven't been delivered
    #
    # @param [Boolean] block_until_confirms Force the method to block until all messages have been acked or nacked.
    # @yield [message_id] Calls the given block for any message that was undeliverable (if block_until_confirms was `true`)
    # @yieldparam [String] message_id The message_id of the message which could not be delivered
    def self.close_connections(block_until_confirms: true)
      @@connections.each do |k, c|
        if block_until_confirms && !c.wait_for_confirms
          c.nacked_set.each do |message_id|
            yield message_id if block_given?
          end
        end
        c.close
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
        @data.to_json
      end

      def ==(other)
        self.to_hash == other.to_hash
      rescue
        # Any errors would be because the other isn't a hash, so the answer must be false
        false
      end

      def inspect
        classification_string = @data["classification"].map do |cl| 
          "#{cl["realm"]}:#{cl["id"]}"
        end.join(", ")
        "<#{self.class.name.split("::").last}: #{classification_string}>"
      rescue
        to_s
      end
    end

    class UndeliverableMessageError < RuntimeError; end

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
    # @param [String] path The path to a (or a folder of) json-schema file(s) in the blinkbox Books format.
    # @param [String] root The root path from which namespaces will be calculated. 
    # @return Array of class names generated
    def self.init_from_schema_at(path, root = path)
      fail "The path #{path} does not exist" unless File.exist?(path)
      return Dir[File.join(path, "**/*.schema.json")].map { |file| init_from_schema_at(file, root) }.flatten if File.directory?(path)

      root = File.dirname(root) if root =~ /\.schema\.json$/
      schema_name = path.sub(%r{^(?:\./)?#{root}/?(.+)\.schema\.json$}, "\\1").tr("/",".")
      class_name = class_name_from_schema_name(schema_name)

      # We will re-declare these classes if required, rather than raise an error.
      remove_const(class_name) if constants.include?(class_name.to_sym)

      const_set(class_name, Class.new {
        include JsonSchemaPowered

        def initialize(data = {})
          @data = data
          @data = @data.stringify_keys if data.respond_to?(:stringify_keys)
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
      fail "No content type was given" if content_type.nil? || content_type.empty?
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
