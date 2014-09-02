require "tmpdir"
require "fileutils"
require "json"

context Blinkbox::CommonMessaging do
  describe "#init_from_schema_at" do
    subject(:klass) {
      @dir = Dir.mktmpdir
      path = File.join(@dir,"namespace/to/example")
      FileUtils.mkdir_p(path)
      open(File.join(path, "v1.schema.json"),"w") do |f|
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

    after :each do
      FileUtils.remove_entry_secure @dir
    end

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
        expect { klass.new(data) }.to raise_error(JSON::Schema::ValidationError)
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
  end
end