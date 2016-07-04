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
#   Line 36-47: Added our default PNDA schema
#   Line 49-50: Introduced 'platformavro' class extending from base codec class.
#   Line 58, 61: Introduced class variables.
#   register method (Line 76-83): Modified register method to read schema from a URI if provided or default to the PNDA schema.
#   decode method (Line 92-129): Decoding logstash events as per the schema.
#   encode method (Line 140-157): Encoding logstash events as per the schema.
#   parse_addr method (Line 169-181): Parse the host field to seperate IP address & host, if exists.
#
#   Name:    platformavro.rb
#   Purpose: This plugin is used to serialize or deserialize logstash events in accrodance with the PNDA avro schema.
#   Author:  PNDA team

require "open-uri"
require "avro"
require "logstash/codecs/base"
require "logstash/event"
require "logstash/timestamp"
require "logstash/util"

DEFAULT_SCHEMA_ID = 1
DEFAULT_SRC = ""
DEFAULT_SCHEMA = <<-JSON
{"namespace": "pnda.entity",
 "type": "record",
 "name": "event",
 "fields": [
     {"name": "timestamp", "type": "long"},
     {"name": "src",       "type": "string", "default": "#{DEFAULT_SRC}" },
     {"name": "host_ip",   "type": "string"},
     {"name": "rawdata",   "type": "bytes"}
 ]
}
JSON

class LogStash::Codecs::Platformavro < LogStash::Codecs::Base
  config_name "platformavro"

  # schema path to fetch the schema from
  # This can be a 'http' or 'file' scheme URI
  # example:
  # http - "http://example.com/schema.avsc"
  # file - "/path/to/schema.avsc"
  config :schema_uri, :validate => :string
  config :schema_id, :validate => :number, :default => DEFAULT_SCHEMA_ID

  # Use a fake ip address (v4 or v6) as a source ip
  config :fake_debug_ip, :validate => :string

  # Public: Read the avro schema from a file or HTTP URI.
  #
  # uri_string  - The path containing the schema file.
  #
  # Examples
  #
  #   open_and_read('/home/user/schema.avsc')
  #
  def open_and_read(uri_string)
    open(uri_string).read
  end

  public
  def register
    if @schema_uri
      schema = open_and_read(schema_uri)
    else
      schema = DEFAULT_SCHEMA
    end
    @schema = Avro::Schema.parse(schema)
  end

  # ==== Decoding
  #
  # This is for deserializing individual Avro records. 
  #
  # data - Avro encoded data.
  #
  public
  def decode(data)
    got_error = false
    begin
      schema_id = DEFAULT_SCHEMA_ID
      datum = StringIO.new(data)
      decoder = Avro::IO::BinaryDecoder.new(datum)
      datum_reader = Avro::IO::DatumReader.new(@schema)
      decoded = datum_reader.read(decoder)
      # create a new event object
      event = LogStash::Event.new
      # get the avro decoded fields
      event["src"] = decoded["src"].force_encoding("UTF-8")
      event["host"] = decoded["host_ip"].force_encoding("UTF-8")
      tmpDate = Time.at(decoded["timestamp"] / 1000)
      event["@timestamp"] = LogStash::Timestamp.new(tmpDate)
      event["timestamp"] = decoded["timestamp"]
      # avromessage event contains the full decoded object.
      event["avromessage"] = decoded

      # Populate the message event
      event["message"] = decoded["rawdata"].force_encoding("UTF-8")
      event["schema_id"] = schema_id
    rescue => e
      got_error = true
      @logger.warn("Trouble parsing Avro input, falling back to plain text",
                   :input => data, :exception => e)
      eventError = LogStash::Event.new
      eventError["message"] = data.force_encoding("UTF-8")
      eventError["tags"] ||= []
      eventError["tags"] << "_avroparsefailure"
      eventError["tags"] << e
    end
    if got_error
      yield eventError
    else
      yield event
    end
  end

  # ==== Encoding
  # 
  # This method is for serializing individual Logstash events 
  # as Avro datums that are Avro binary blobs. 
  # Writes to a datumWriter object.
  #
  # event - Logstash event.
  #
  public
  def encode(event)
    datum_writer = Avro::IO::DatumWriter.new(@schema)
    buffer = StringIO.new
    encoder = Avro::IO::BinaryEncoder.new(buffer)
    # get the IP address from host field
    address, port = parse_addr(event["host"])
    # create the data object
    data = {
      "timestamp" => event["@timestamp"].to_i * 1000,
      "src" => event["src"] || DEFAULT_SRC,
      "host_ip" => @fake_debug_ip || address,
      "rawdata" => event["message"]
    }
    datum_writer.write(data, encoder)

    @on_event.call(event, buffer.string.to_java_bytes)

  end

  # Public: Split IP address and port number if found in the string.
  #
  # host  - The IP address string.
  #
  # Examples
  #
  #   parse_addr('173.39.242.45')
  # 
  # Returns the IP address and the port number(if present).
  #
  def parse_addr(host)
      case host
      when /\A\[(?<address> .* )\]:(?<port> \d+ )\z/x      # "[::1]:80"
        address, port = $~[:address], $~[:port]
      when /\A(?<address> [^:]+ ):(?<port> \d+ )\z/x       # "127.0.0.1:80"
        address, port = $~[:address], $~[:port]
      else                                                 # no port number
        address, port = host, nil
      end
      return address, port
  end

end
