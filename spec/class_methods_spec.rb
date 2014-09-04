require "tmpdir"
require "fileutils"
require "json"

context Blinkbox::CommonMessaging do
  describe "#init_from_schema_at" do
    before :all do
      @dir = Dir.mktmpdir
      @content_type = "application/vnd.blinkbox.books.namespace.to.example.v1+json"
      @schema_path = "namespace/to/example/v1.schema.json"
    end

    after :all do
      FileUtils.remove_entry_secure @dir
    end

    subject(:klass) {
      path = File.join(@dir, File.dirname(@schema_path))
      FileUtils.mkdir_p(path)
      open(File.join(path, File.basename(@schema_path)),"w") do |f|
        schema = {
          title: "A basic JSON schema file for tests",
          type: "object",
          properties: {
            requiredField: {
              type: "string"
            },
            optionalField: {
              type: "integer"
            },
            defaultField: {
              type: "string",
              default: "default value"
            }
          },
          additionalProperties: false,
          required: ["requiredField"]
        }
        f.write schema.to_json
      end

      klasses = described_class.init_from_schema_at(@dir)
      klasses.first
    }

    it "must create classes for every json schema file in a folder" do
      expect(klass).to eql(Blinkbox::CommonMessaging::NamespaceToExampleV1)
      expect(klass.included_modules).to include(Blinkbox::CommonMessaging::JsonSchemaPowered)
    end

    describe "a messaging class" do
      it "must initialize if attributes are valid" do
        data = {
          requiredField: "I'm a string",
          optionalField: 123
        }
        instance = nil
        expect {
          instance = klass.new(data)
        }.to_not raise_error

        expect(instance).to be_a(klass)
      end

      it "must initialize if optional attributes are missing" do
        data = {
          requiredField: "I'm a string"
        }
        instance = nil
        expect {
          instance = klass.new(data)
        }.to_not raise_error

        expect(instance).to be_a(klass)
      end

      it "must not initialize if required attributes are missing" do
        data = {
          optionalField: 123
        }
        expect { klass.new(data) }.to raise_error
      end

      it "must not initialize if attributes are of the wrong type" do
        data = {
          optionalField: "not an integer"
        }
        expect { klass.new(data) }.to raise_error
      end

      it "must respond with values when using #[]" do
        data = {
          requiredField: "I'm a string"
        }
        instance = klass.new(data)

        expect(instance[:requiredField]).to eq(data[:requiredField])
      end

      it "must add default values" do
        data = {
          requiredField: "I'm a string"
        }
        instance = klass.new(data)

        expect(instance[:defaultField]).to eq("default value")
      end

      it "must have the correct content_type" do
        data = {
          requiredField: "I'm a string",
          optionalField: 123
        }
        instance = klass.new(data)

        expect(instance.content_type).to eq("application/vnd.blinkbox.books.namespace.to.example.v1+json")
      end

      it "must render to json as the source hash" do
        data = {
          requiredField: "I'm a string",
          optionalField: 123,
          defaultField: "Overwritten value"
        }
        instance = klass.new(data)

        expect(instance.to_json).to eq(data.to_json)
      end
    end

    describe "#class_from_content_type" do
      it "must return the class for a content-type which has been 'init'ed" do
        received = described_class.class_from_content_type(@content_type)
        expect(received).to eq(Blinkbox::CommonMessaging::NamespaceToExampleV1)
      end

      it "must return a runtime error if no content-type is given" do
        [nil, ""].each do |value|
          expect{
            described_class.class_from_content_type(value)
          }.to raise_error(RuntimeError), "No content type was given (#{value.inspect}). This should have raised an error."
        end
      end

      it "must return a runtime error if the specified content type has not been 'init'ed" do
        expect{
          described_class.class_from_content_type("application/vnd.not.an.inited.type")
        }.to raise_error(RuntimeError), "The content type given wasn't initialised, no class should be returned"
      end
    end
  end

  describe "#configure" do
    it "must update the bunny host, port, user, pass and vhost from delivered hash" do
      uri = URI("amqp://user:pass@host:12345/vhost")
      described_class.configure({
        url: uri.to_s
      })

      config = described_class.class_variable_get(:'@@config')
      expect(config[:bunny][:host]).to eq(uri.host)
      expect(config[:bunny][:port]).to eq(uri.port)
      expect(config[:bunny][:user]).to eq(uri.user)
      expect(config[:bunny][:pass]).to eq(uri.password)
      expect(config[:bunny][:vhost]).to eq(uri.path)
    end

    it "must update the retry interval (initial and max) from delivered hash" do
      initial = Unit("1.5 s")
      max = Unit("2.5 s")

      described_class.configure({
        initialRetryInterval: initial,
        maxRetryInterval: max
      })

      config = described_class.class_variable_get(:'@@config')
      expect(config[:retry_interval][:initial]).to eq(initial)
      expect(config[:retry_interval][:max]).to eq(max)
    end
  end

  describe "#config" do
    it "must return the default config if no call to #configure has been made" do
      expect(described_class.config).to eql(described_class::DEFAULT_CONFIG)
    end

    it "must return the config if a call to #configure has been made" do
      described_class.configure({
        url: "amqp://user:pass@host:12345/vhost"
      })

      expect(described_class.config).to eql(described_class.class_variable_get(:'@@config'))
    end
  end

  describe "#logger=" do
    before :all do
      class FakeLogger
        def initialize(respond = nil)
          @respond = respond
        end

        def respond_to?(m)
          (m != @respond)
        end
      end
    end

    it "must set the class logger" do
      logger = FakeLogger.new()
      described_class.logger = logger
      expect(described_class.class_variable_get(:'@@config')[:logger]).to eq(logger)
    end

    it "should raise an error if the given variable isn't logger compatible" do
      logger_methods = %i{debug info warn error fatal level= level}

      logger_methods.each do |method|
        fake_logger = FakeLogger.new(method)
        expect{ described_class.logger = fake_logger }.to raise_error(ArgumentError), "A logger without the '#{method}' instance method should be rejected"
      end
    end
  end

  describe "#connection" do
    before :each do
      allow(Bunny).to receive(:new).and_return(
        # Return a different instance of the doube the second time around
        double(Bunny::Session, create_channel: nil, start: nil),
        double(Bunny::Session, create_channel: nil, start: nil)
      )
    end

    after :each do 
      described_class.class_variable_set(:'@@connections', {})
    end

    it "must return a started Bunny::Connection" do
      expect(described_class.connection).to respond_to(:create_channel)
    end

    it "must return a different Bunny::Connection if @@config changes" do
      described_class.configure(url: "amqp://user:pass@first:54321/vhost")
      first_connection = described_class.connection
      described_class.configure(url: "amqp://user:pass@second:54321/vhost")
      expect(described_class.connection).to_not eql(first_connection)
    end
  end

  describe "#class_name_from_scheme_name" do

  end
end