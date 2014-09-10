context Blinkbox::CommonMessaging::Exchange do
  describe "#initialize" do
    before :each do
      @doubles = {
        connection: double(Bunny::Session),
        channel: double(Bunny::Channel),
        exchange: double(Bunny::Exchange)
      }

      allow(@doubles[:channel]).to receive(:headers).and_return(@doubles[:exchange])
      allow(@doubles[:channel]).to receive(:confirm_select)
      allow(@doubles[:connection]).to receive(:create_channel).and_return(@doubles[:channel])
      allow(@doubles[:connection]).to receive(:start)

      allow(Bunny).to receive(:new).and_return(@doubles[:connection])
    end

    it "must create a new headers exchange of the given name" do
      exchange_name = "testing"
      exchange = described_class.new(exchange_name)

      expect(exchange).to be_a(described_class)
      expect(@doubles[:channel]).to have_received(:headers).with(exchange_name, durable: true, auto_delete: false, passive: true)
    end

    it "must set the @app_id variable" do
      exchange_name = "testing2"
      facility = "Rspec.test"
      facility_version = "0.0.0-rspec"
      exchange = described_class.new(exchange_name, facility: facility, facility_version: facility_version)

      expect(exchange.instance_variable_get(:'@app_id')).to eq("#{facility}:v#{facility_version}")
    end
  end

  describe ".publish" do
    before :each do
      @real_exchange = double("exchange")
      @real_channel = double("channel")
      allow(@real_exchange).to receive(:publish)
      allow(@real_exchange).to receive(:channel).and_return(@real_channel)
      allow(@real_channel).to receive(:wait_for_confirms).and_return(true)
      allow(@real_channel).to receive(:nacked_set).and_return(Set.new(["abc123"]))

      @exchange = described_class.allocate
      @exchange.instance_variable_set(:'@exchange', @real_exchange)

      @object = @klass.new(@valid_object)
    end

    it "must publish the message object in json format to the exchange" do
      @exchange.publish(@object)
      expect(@real_exchange).to have_received(:publish).with(@object.to_json, anything)
    end

    it "must raise an error when the message object isn't JsonSchemaPowered" do
      expect {
        @exchange.publish("Not a JSON Schema Powered Object")
      }.to raise_error ArgumentError
    end

    it "must add a content-type header (as well as the property)" do
      @exchange.publish(@object)
      expect(@real_exchange).to have_received(:publish).with(
        anything,
        hash_including(
          content_type: @object.content_type,
          headers: hash_including(
            "content-type" => @object.content_type
          )
        )
      )
    end

    it "must add the specified headers to the message" do
      extra_headers = {
        "my_awesome_header" => "UNICORNS!",
        "another_header" => "Mathematical!"
      }
      @exchange.publish(@object, headers: extra_headers)
      expect(@real_exchange).to have_received(:publish).with(
        anything,
        hash_including(
          headers: hash_including(extra_headers)
        )
      )
    end

    it "must generate a message_id that is 16 bytes of hexadecimal chars in ascii" do
      @exchange.publish(@object)
      expect(@real_exchange).to have_received(:publish).with(
        anything,
        hash_including(
          message_id: /^[0-9a-f]{16}$/
        )
      )
    end

    it "must return the message_id" do
      message_id = @exchange.publish(@object)
      expect(@real_exchange).to have_received(:publish).with(
        anything,
        hash_including(
          message_id: message_id
        )
      )
    end

    it "must pass on the message_id_chain, appending the generated message_id" do
      message_id_chain = %w{123 456}

      message_id = @exchange.publish(@object, message_id_chain: message_id_chain.dup)
      expect(@real_exchange).to have_received(:publish).with(
        anything,
        hash_including(
          headers: hash_including(
            "message_id_chain" => message_id_chain + [message_id]
          )
        )
      )
    end

    it "must set the correlation_id to be the message_id if no message chain is passed" do
      message_id = @exchange.publish(@object)
      expect(@real_exchange).to have_received(:publish).with(
        anything,
        hash_including(
          correlation_id: message_id
        )
      )
    end

    it "must set the correlation_id to be the first message_id if a message chain is passed" do
      message_id_chain = %w{123 456}

      @exchange.publish(@object, message_id_chain: message_id_chain)
      expect(@real_exchange).to have_received(:publish).with(
        anything,
        hash_including(
          correlation_id: message_id_chain.first
        )
      )
    end

    it "must send persistent messages" do
      @exchange.publish(@object)
      expect(@real_exchange).to have_received(:publish).with(
        anything,
        hash_including(
          persistent: true
        )
      )
    end

    it "must send messages requiring publisher confirms by default" do
      @exchange.publish(@object)
      expect(@real_channel).to have_received(:wait_for_confirms)
    end

    it "must allow messages that do not require publisher confirms" do
      @exchange.publish(@object, confirm: false)
      expect(@real_channel).to_not have_received(:wait_for_confirms)
    end

    it "must not raise exceptions if the message is confirmed" do
      @exchange.publish(@object)
      expect(@real_channel).to_not have_received(:nacked_set)
    end

    it "must raise an UndeliverableMessageError if the message is not confirmed" do
      # Call wait_for_confirms once, and receive the 'true' which is queued up
      @real_channel.wait_for_confirms
      # Begin with the real test
      allow(@real_channel).to receive(:wait_for_confirms).and_return(false)
      expect {
        @exchange.publish(@object)
      }.to raise_error(Blinkbox::CommonMessaging::UndeliverableMessageError)
      expect(@real_channel).to have_received(:nacked_set)
    end

    it "must set the app_id to the facility and facility_version given at instantiation" do
      app_id = "bloop:v0.0.0-bloop"
      @exchange.instance_variable_set(:'@app_id', app_id)
      @exchange.publish(@object)
      expect(@real_exchange).to have_received(:publish).with(
        anything,
        hash_including(
          app_id: app_id
        )
      )
    end
  end
end
