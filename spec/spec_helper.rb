$LOAD_PATH.unshift File.join(__dir__, "../lib")
require "blinkbox/common_messaging"

module Helpers
  
end

RSpec.configure do |c|
  c.include Helpers
end