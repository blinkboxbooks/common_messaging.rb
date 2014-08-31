$:<<"lib"
require "blinkbox/common_messaging"
require "json"

Blinkbox::CommonMessaging.init_from_schema_at("/Users/jp/Projects/schemas/")
x = Blinkbox::CommonMessaging::Exchange.new("Marvin")
d = JSON.load(open("/Users/jp/Dropbox/MobcastProjects/schemas/ingestion/book/metadata/cover_processor.v2.json"))
m = Blinkbox::CommonMessaging::IngestionBookMetadataV2.new(d)

#############

q = Blinkbox::CommonMessaging::Queue.new(
  "Marvin.cover_processor",
  exchange: "Marvin",
  bindings: ["image/jpeg", "image/png", "image/tiff"].map { |content_type|
    {
      "content-type" => "application/vnd.blinkbox.books.ingestion.book.metadata.v2+json",
      "referenced-content-type" => content_type,
      "x-match" => "all"
    }
  }
)

p x.publish(m, headers: { 'referenced-content-type' => "image/jpeg" })