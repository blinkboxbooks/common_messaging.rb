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
        double(Bunny::Session, create_channel: @doubles[:connection], start: nil)
      )
    end

    after :each do 
      Blinkbox::CommonMessaging.class_variable_set(:'@@connections', {})
    end

    it "must create a new queue of the given name" do
      queue_name = "testing"
      queue = Blinkbox::CommonMessaging::Queue.new(queue_name)

      expect(queue).to be_a(Blinkbox::CommonMessaging::Queue)
      expect(@doubles[:connection]).to have_received(:queue).with(queue_name, {durable: true, auto_delete: false, exclusive: false})
    end

    it "must bind the queue to the given exchange with each of the given binding headers" do
      exchange_name = "exchange"
      bindings = [{ 'key' => 'value' }, { 'key2' => 'value2' }]
      Blinkbox::CommonMessaging::Queue.new("whatever", exchange: exchange_name, bindings: bindings)

      bindings.each do |binding|
        expect(@doubles[:queue]).to have_received(:bind).with(@doubles[:exchange], arguments: binding)
      end
    end

    it "should raise a warning if no bindings are given" do
      allow(Kernel).to receive(:warn)
      Blinkbox::CommonMessaging::Queue.new("fizz", exchange: "buzz", bindings: [])
      expect(Kernel).to have_received(:warn)
    end
  end

  describe ".subscribe" do

  end
end