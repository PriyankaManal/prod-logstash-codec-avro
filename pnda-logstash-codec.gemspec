Gem::Specification.new do |s|

  s.name            = 'logstash-codec-platformavro'
  s.version         = '0.1.0'
  s.licenses        = ['Apache License (2.0)']
  s.summary         = "Encode and decode avro formatted data - PNDA data bus"
  s.description     = "Kafka Producer shall use this codec to avro encode input messages. Kafka Consumer shall use this codec to avro decode messages read from the data bus."
  s.authors         = ["PNDA team"]
  s.require_paths   = ["lib"]

  # Files
  s.files = Dir['*', 'lib/logstash/codecs/*', 'spec/codecs/*']

  # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "codec" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core", '>= 1.4.0', '< 3.0.0'

  s.add_runtime_dependency "avro"

  #s.add_development_dependency 'logstash-devutils', '~> 0'
  s.add_development_dependency 'logstash-devutils'
end

