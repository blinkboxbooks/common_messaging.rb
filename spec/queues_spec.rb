context Blinkbox::CommonMessaging::Queue do
  describe "#initialize" do
    before :each do
      @doubles = {
        queue: double(Bunny::Queue),
        connection: double(Bunny::Channel),
        exchange: double(Bunny::Exchange)
      }

      allow(@doubles[:connection]).to receive(:queue).and_return(@doubles[:queue])
      allow(@doubles[:connection]).to receive(:headers).and_return(@doubles[:exchange])
      allow(@doubles[:queue]).to receive(:bind)

      allow(Bunny).to receive(:new).and_return(
        double(Bunny::Session, create_channel: @doubles[:connection], start: nil, confirm_select: nil)
      )
    end

    it "must create a new queue of the given name" do
      queue_name = "testing"
      queue = described_class.new(queue_name)

      expect(queue).to be_a(described_class)
      expect(@doubles[:connection]).to have_received(:queue).with(
        queue_name,
        durable: true,
        auto_delete: false,
        exclusive: false,
        arguments: {
          "x-dead-letter-exchange" => anything
        }
      )
    end

    it "must bind the queue to the given exchange with each of the given binding headers" do
      exchange_name = "exchange"
      bindings = [{ 'key' => 'value' }, { 'key2' => 'value2' }]
      Blinkbox::CommonMessaging::Queue.new("whatever", exchange: exchange_name, bindings: bindings)

      bindings.each do |binding|
        binding["x-dead-letter-exchange"] = "#{exchange_name}.DLX"
        expect(@doubles[:queue]).to have_received(:bind).with(@doubles[:exchange], arguments: binding)
      end
    end

    it "should raise a warning if no bindings are given" do
      allow(Kernel).to receive(:warn)
      described_class.new("fizz", exchange: "buzz", bindings: [])
      expect(Kernel).to have_received(:warn)
    end
  end

  describe ".subscribe" do
    before :each do
      @dummy_data = {
        delivery_info: {
          delivery_tag: "TAGTASTIC!"
        },
        metadata: {
          headers: {
            'content-type' => @content_type
          }
        },
        payload: @valid_object.to_json
      }

      @queue = described_class.allocate
      bunny_queue = double("real_queue")
      allow(bunny_queue).to receive(:subscribe).and_yield(@dummy_data[:delivery_info], @dummy_data[:metadata], @dummy_data[:payload])
      @queue.instance_variable_set(:'@queue', bunny_queue)

      @bunny_channel = double("real_channel")
      allow(@bunny_channel).to receive(:ack)
      allow(@bunny_channel).to receive(:reject)
      @queue.instance_variable_set(:'@channel', @bunny_channel)

      @logger = double(Logger.new(nil))
      allow(@logger).to receive(:error)
      @queue.instance_variable_set(:'@logger', @logger)
    end

    it "must subscribe to the queue and yield metadata and the message object" do
      message_handler = proc { |metadata, message_object|
        expect(metadata).to eq(@dummy_data[:metadata])
        expect(message_object).to be_a(@klass)
        :ack
      }

      @queue.subscribe(block: false, &message_handler)
    end

    it "must acknowledge the message when the passed block returns :ack" do
      message_handler = proc { |_metadata, _message_object|
        :ack
      }

      @queue.subscribe(block: false, &message_handler)
      expect(@bunny_channel).to have_received(:ack).with(@dummy_data[:delivery_info][:delivery_tag])
    end

    it "must acknowledge the message when the passed block returns true" do
      message_handler = proc { |_metadata, _message_object|
        true
      }

      @queue.subscribe(block: false, &message_handler)
      expect(@bunny_channel).to have_received(:ack).with(@dummy_data[:delivery_info][:delivery_tag])
    end

    it "must reject & request the message delivery be retried when the passed block returns :retry" do
      message_handler = proc { |_metadata, _message_object|
        :retry
      }

      @queue.subscribe(block: false, &message_handler)
      expect(@bunny_channel).to have_received(:reject).with(@dummy_data[:delivery_info][:delivery_tag], true)
    end

    it "must reject without retrying (ie. DLQ) the message when the passed block returns :reject" do
      message_handler = proc { |_metadata, _message_object|
        :reject
      }

      @queue.subscribe(block: false, &message_handler)
      expect(@bunny_channel).to have_received(:reject).with(@dummy_data[:delivery_info][:delivery_tag], false)
    end

    it "must reject without retrying (ie. DLQ) the message when the passed block returns false" do
      message_handler = proc { |_metadata, _message_object|
        false
      }

      @queue.subscribe(block: false, &message_handler)
      expect(@bunny_channel).to have_received(:reject).with(@dummy_data[:delivery_info][:delivery_tag], false)
    end

    it "must call the `on_exception` method for messages that raise an exception in the passed block" do
      exception = Exception.new("An error of any variety that inherrits from Exception")
      message_handler = proc { |_metadata, _message_object|
        raise exception
      }
      on_exception = proc do |e, channel, delivery_info, metadata, payload|
        expect(e).to eql(exception)
        expect(channel).to be_a(Bunny::Connection)
        expect(delivery_info).to eq(@dummy_data[:delivery_info])
        expect(metadata).to eq(@dummy_data[:metadata])
        expect(payload).to eq(@dummy_data[:payload])
      end

      @queue.subscribe(block: false, &message_handler)
    end
  end
end
