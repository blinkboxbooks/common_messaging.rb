$LOAD_PATH.unshift File.join(__dir__, "../lib")
require "blinkbox/common_messaging"
require "bunny_mock"

module Helpers
  
end

RSpec.configure do |c|
  c.include Helpers
end