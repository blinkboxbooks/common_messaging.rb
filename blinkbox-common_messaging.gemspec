# -*- encoding: utf-8 -*-
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), "lib"))
require "blinkbox/common_messaging/version"

Gem::Specification.new do |gem|
  gem.name          = "blinkbox-common_messaging"
  gem.version       = Blinkbox::CommonMessaging::VERSION
  gem.authors       = ["JP Hastings-Spital"]
  gem.email         = ["jphastings@blinkbox.com"]
  gem.description   = %q{Simple helper for messaging around blinkbox Books}
  gem.summary       = %q{Simple helper for messaging around blinkbox Books}
  gem.homepage      = ""

  gem.files         = Dir["lib/**/*.rb", "VERSION"]
  gem.extra_rdoc_files = Dir["**/*.md"]
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_dependency "bunny", "~>1.4"
  gem.add_dependency "activesupport"
  gem.add_dependency "ruby-units", "~>1.4"
  gem.add_dependency "json-schema"

  gem.add_development_dependency "rake"
  gem.add_development_dependency "rspec", "~>3.0"
  gem.add_development_dependency "simplecov"
end
