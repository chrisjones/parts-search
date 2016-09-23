require 'elasticsearch'

client = Elasticsearch::Client.new log: true

client.search q: "#{ARGV[0]}"
