$LOAD_PATH.unshift File.join(__dir__, "../lib")
require "blinkbox/common_messaging"

RSpec.configure do |config|
  config.before :all do
    @dir = Dir.mktmpdir
    @content_type = "application/vnd.blinkbox.books.namespace.to.example.v1+json"
    @schema_path = "namespace/to/example/v1.schema.json"

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

    klasses = Blinkbox::CommonMessaging.init_from_schema_at(@dir)
    @klass = klasses.first
  end

  config.after :all do
    FileUtils.remove_entry_secure @dir
  end
end