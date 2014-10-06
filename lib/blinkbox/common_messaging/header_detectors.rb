class Blinkbox::CommonMessaging::HeaderDetectors
  @@header_detectors = []

  def initialize(obj)
    @obj = obj
  end

  def modified_headers(original_headers = {})
    @@header_detectors.each do |m|
      original_headers = send(m, original_headers)
    end
    original_headers
  end

  def self.register(method_name)
    @@header_detectors << method_name
  end
end

Dir.glob(File.join(__dir__, "header_detectors/*.rb")) { |hd| require hd }