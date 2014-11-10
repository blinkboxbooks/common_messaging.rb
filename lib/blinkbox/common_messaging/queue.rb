module Blinkbox
  module CommonMessaging
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
      # @param [String] queue_name The name of the queue which should be used and (if necessary) created.
      # @param [String] exchange The name of the Exchange to bind to. The default value should be avoided for production uses.
      # @param [String] dlx The name of the Dead Letter Exchange to send nacked messages to.
      # @param [Array,Hash] bindings An array of hashes, each on detailing the parameters for a new binding.
      # @param [Integer] prefetch The number of messages to collect at a time when subscribing.
      # @raise [Bunny::NotFound] If the exchange does not exist.
      # @return [Bunny::Queue] A blinkbox managed Bunny Queue object
      def initialize(queue_name, exchange: "amq.headers", dlx: "#{exchange}.DLX", bindings: [], prefetch: 10)
        raise ArgumentError, "Prefetch must be a positive integer" unless prefetch.is_a?(Integer) && prefetch > 0
        connection = CommonMessaging.connection
        @logger = CommonMessaging.config[:logger]
        # We create one channel per queue because it means that any issues are isolated
        # and we can start a new channel and resume efforts in a segregated manner.
        @channel = connection.create_channel
        @channel.prefetch(prefetch)
        @queue = @channel.queue(
          queue_name,
          durable: true,
          auto_delete: false,
          exclusive: false,
          arguments: {
            "x-dead-letter-exchange" => dlx
          }
        )
        @exchange = @channel.headers(
          exchange,
          durable: true,
          auto_delete: false,
          passive: true
        )
        Kernel.warn "No bindings were given, the queue is unlikely to receive any messages" if bindings.empty?
        bindings.each do |binding|
          @queue.bind(@exchange, arguments: binding)
        end
      end

      # Defines a new block for handling exceptions which occur when processing an incoming message. Cases where this might occur include:
      #
      # * A message which doesn't have a recognised content-type (ie. one which has been 'init'ed)
      # * An invalid JSON message
      # * A valid JSON message which doesn't pass schema validation
      #
      # @example Sending excepted messages to a log, then nack them
      #   log = Logger.new(STDOUT)
      #   queue = Blinkbox::CommonMessaging::Queue.new("My.Queue")
      #   queue.on_exception do |e, delivery_info, metadata, payload|
      #     log.error e
      #     channel.reject(delivery_info[:delivery_tag], false)
      #   end
      #
      # @yield [exception, channel, delivery)info, metadata, payload] Yields for each exception which occurs.
      # @yieldparam [Exception] exception The exception which was raised.
      # @yieldparam [Bunny::Connection] exception The channel this exchnage is using (useful for nacking).
      # @yieldparam [Hash] delivery_info The RabbitMQ delivery info for the message (useful for nacking).
      # @yieldparam [Hash] metadata The metadata delivered from the RabbitMQ server (parameters and headers).
      # @yieldparam [String] payload The message that was received
      def on_exception(&block)
        raise ArgumentError, "Please specify a block to call when an exception is raised" unless block_given?
        @on_exception = block
      end

      # Emits the metadata and objectified payload for every message which appears on the queue. Any message with a content-type
      # not 'init'ed will be rejected (without retry) automatically.
      #
      # * Returning `true` or `:ack` from the block will acknowledge and remove the message from the queue
      # * Returning `false` or `:reject` from the block will send the message to the DLQ
      # * Returning `:retry` will put the message back on the queue to be tried again later.
      #
      # @example Subscribing to messages
      #   queue = Blinkbox::CommonMessaging::Queue.new("catch-all", exchange_name: "Marvin", [{}])
      #   queue.subscribe(block:true) do |metadata, obj|
      #     puts "Messge received."
      #     puts "Headers: #{metadata[:headers].to_json}"
      #     puts "Body: #{obj.to_json}"
      #   end
      #
      # @param [Boolean] :block Should this method block while being executed (true, default) or spawn a new thread? (false)
      # @param [Array<Blinkbox::CommonMessaging::JsonSchemaPowered>, nil] :accept List of schema types to accept (any not on the list will be rejected). `nil` will accept all message types and not validate incoming messages.
      # @yield [metadata, payload_object] A block to execute for each message which is received on this queue.
      # @yieldparam metadata [Hash] The properties and headers (in [:headers]) delivered with the message.
      # @yieldparam payload_object [Blinkbox::CommonMessaging::JsonSchemaPowered] An object representing the validated JSON payload.
      # @yieldreturn [Boolean, :ack, :reject, :retry]
      def subscribe(block: true, accept: nil)
        raise ArgumentError, "Please give a block to run when a message is received" unless block_given?
        @queue.subscribe(
          block: block,
          manual_ack: true
        ) do |delivery_info, metadata, payload|
          begin
            if accept.nil?
              object = payload
            else
              klass = Blinkbox::CommonMessaging.class_from_content_type(metadata[:headers]['content-type'])
              if accept.include?(klass)
                object = klass.new(JSON.parse(payload)) 
              else
                response = :reject 
              end
            end
            response ||= yield(metadata, object)
            case response
            when :ack, true
              @channel.ack(delivery_info[:delivery_tag])
            when :reject, false
              @channel.reject(delivery_info[:delivery_tag], false)
            when :retry
              @channel.reject(delivery_info[:delivery_tag], true)
            else
              fail "Unknown response from subscribe block: #{response}"
            end
          rescue Exception => e
            (@on_exception || method(:default_on_exception)).call(e, @channel, delivery_info, metadata, payload)
          end
        end
      end

      # Purges all messages from this queue. Destroys data!
      #
      # @return [true] Returns true if the purge occurred correctly (or a RabbitMQ error if it couldn't)
      def purge!
        @queue.purge
        true
      end

      private

      # The default handler for exceptions which occur when processing a message.
      def default_on_exception(exception, channel, delivery_info, metadata, payload)
        @logger.error exception
        channel.reject(delivery_info[:delivery_tag], false)
      end
    end
  end
end