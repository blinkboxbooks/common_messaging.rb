module Blinkbox
  module CommonMessaging
    VERSION = begin
      File.read(File.join(File.dirname(__FILE__), "../../../VERSION"))
    rescue Errno::ENOENT
      "0.0.0-unknown"
    end
  end
end