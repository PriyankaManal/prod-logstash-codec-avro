# encoding: utf-8

#   Copyright (c) 2012–2016 Elasticsearch <http://www.elastic.co>
#   Licensed under the Apache License, Version 2.0 (the ""License"");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an ""AS IS"" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
#   Some Modifications (c) Cisco 2016 under the Apache 2.0 license:
#   Added sample events to test against.
#   Unit tests to prove the plugin works as expected.
#
#   Name:    avro_spec.rb
#   Purpose: Unit test case for the platform avro codec plugin.
#   Author:  PNDA team

require 'logstash/devutils/rspec/spec_helper'
require 'avro'
require 'logstash/codecs/platformavro'
require 'logstash/event'

describe LogStash::Codecs::Platformavro do
  let (:play_data) { {"timestamp" => 1436451599000, "src" => "avro.log.unittest", "host_ip" => "192.168.0.1", "rawdata" => "abc"} }
  let (:input_event) { LogStash::Event.new({"@timestamp" => "2015-07-09T14:19:59.000Z", "src" => "avro.log.unittest", "host" => "192.168.0.1", "message" => "abc"}) }
  let (:input_event2) { LogStash::Event.new({"@timestamp" => "2015-07-09T14:19:59.000Z", "src" => "avro.log.unittest", "host" => "192.168.0.1:2001", "message" => "abc"}) }
  let (:input_event3) { LogStash::Event.new({"@timestamp" => "2015-07-09T14:19:59.000Z", "src" => "avro.log.unittest", "host" => "www.unit.test", "message" => "abc"}) }
  let (:play_data3) { {"timestamp" => 1436451599000, "src" => "avro.log.unittest", "host_ip" => "www.unit.test", "rawdata" => "abc"} }
  let (:input_event4) { LogStash::Event.new({"@timestamp" => "2015-07-09T14:19:59.000Z", "src" => "avro.log.unittest", "host" => "www.unit.test:80", "message" => "abc"}) }
  
  subject do
    allow_any_instance_of(LogStash::Codecs::Platformavro).to \
      receive(:open_and_read).and_return()
    next LogStash::Codecs::Platformavro.new()
  end

  context "#decode" do
    it "should return an LogStash::Event from avro data" do
      schema = Avro::Schema.parse(DEFAULT_SCHEMA)
      dw = Avro::IO::DatumWriter.new(schema)
      buffer = StringIO.new
      encoder = Avro::IO::BinaryEncoder.new(buffer)
      dw.write(play_data, encoder)

      subject.decode(buffer.string) do |event|
        insist { event.is_a? LogStash::Event }
        insist { event["src"] }    == play_data["src"]
        insist { event["host"] }      == play_data["host_ip"]
        insist { event["message"] }   == play_data["rawdata"]
        insist { event["timestamp"] } == play_data["timestamp"]

      end
    end
  end



  context "#encode (IP@)" do
    it "should return avro data from a LogStash::Event" do
      got_event = false
      subject.on_event do |event, data|

        schema = Avro::Schema.parse(DEFAULT_SCHEMA)
        datum = StringIO.new(data.to_s)
        decoder = Avro::IO::BinaryDecoder.new(datum)
        datum_reader = Avro::IO::DatumReader.new(schema)
        record = datum_reader.read(decoder)

        insist { event.is_a? LogStash::Event }
        insist { record["timestamp"] } == play_data["timestamp"]
        insist { record["src"] }       == play_data["src"]
        insist { record["host_ip"] }   == play_data["host_ip"]
        insist { record["rawdata"] }   == play_data["rawdata"]

        got_event = true
      end
      subject.encode(input_event)
      insist { got_event }
    end
  end


  context "#encode  (IP@ and port)" do
    it "should return avro data from a LogStash::Event" do
      got_event = false
      subject.on_event do |event, data|

        schema = Avro::Schema.parse(DEFAULT_SCHEMA)
        datum = StringIO.new(data.to_s)
        decoder = Avro::IO::BinaryDecoder.new(datum)
        datum_reader = Avro::IO::DatumReader.new(schema)
        record = datum_reader.read(decoder)

        insist { event.is_a? LogStash::Event }
        insist { record["timestamp"] } == play_data["timestamp"]
        insist { record["src"] }       == play_data["src"]
        insist { record["host_ip"] }   == play_data["host_ip"]
        insist { record["rawdata"] }   == play_data["rawdata"]

        got_event = true
      end
      subject.encode(input_event2)
      insist { got_event }
    end
  end

  context "#encode  (hostname)" do
    it "should return avro data from a LogStash::Event" do
      got_event = false
      subject.on_event do |event, data|

        schema = Avro::Schema.parse(DEFAULT_SCHEMA)
        datum = StringIO.new(data.to_s)
        decoder = Avro::IO::BinaryDecoder.new(datum)
        datum_reader = Avro::IO::DatumReader.new(schema)
        record = datum_reader.read(decoder)

        insist { event.is_a? LogStash::Event }
        insist { record["timestamp"] } == play_data3["timestamp"]
        insist { record["src"] }       == play_data3["src"]
        insist { record["host_ip"] }   == play_data3["host_ip"]
        insist { record["rawdata"] }   == play_data3["rawdata"]

        got_event = true
      end
      subject.encode(input_event3)
      insist { got_event }
    end
  end

  context "#encode  (hostname, port)" do
    it "should return avro data from a LogStash::Event" do
      got_event = false
      subject.on_event do |event, data|

        schema = Avro::Schema.parse(DEFAULT_SCHEMA)
        datum = StringIO.new(data.to_s)
        decoder = Avro::IO::BinaryDecoder.new(datum)
        datum_reader = Avro::IO::DatumReader.new(schema)
        record = datum_reader.read(decoder)

        insist { event.is_a? LogStash::Event }
        insist { record["timestamp"] } == play_data3["timestamp"]
        insist { record["src"] }       == play_data3["src"]
        insist { record["host_ip"] }   == play_data3["host_ip"]
        insist { record["rawdata"] }   == play_data3["rawdata"]

        got_event = true
      end
      subject.encode(input_event4)
      insist { got_event }
    end
  end

end
