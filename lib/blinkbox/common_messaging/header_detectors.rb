class Blinkbox::CommonMessaging::HeaderDetectors
  METHODS = []

  def initialize(obj)
    @obj = obj
  end

  def modified_headers(original_headers = {})
    METHODS.each do |m|
      original_headers = send(m, original_headers)
    end
    original_headers
  end
end

Dir.glob(File.join(__dir__, "header_detectors/*.rb")) { |hd| require hd }