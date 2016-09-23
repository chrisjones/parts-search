require 'sinatra'
require 'sinatra/reloader'
require 'logger'
require 'elasticsearch'
require 'json'

::Logger.class_eval { alias :write :'<<' }
access_log = ::File.join(::File.dirname(::File.expand_path(__FILE__)),'log',"#{settings.environment}_access.log")
access_logger = ::Logger.new(access_log)
error_logger = ::File.new(::File.join(::File.dirname(::File.expand_path(__FILE__)),'log',"#{settings.environment}_error.log"),"a+")
error_logger.sync = true

configure do
  ES = Elasticsearch::Client.new(hosts: "10.10.10.8:9200", log: true)
  use ::Rack::CommonLogger, access_logger
end

before {
  env["rack.errors"] =  error_logger
}

class Skus
  def self.match(location: 'bham', text: 'dryer', size: 100)
    ES.search index: location, 
              q: text, 
              size: size 
  end
end

get "/" do
  erb :index
end

get "/search" do
  @term = params[:q].gsub(/[<.)(;*?>]/, '' )
  @index = params[:location]

  @parts = Skus.match(location: @index, text: @term)["hits"]["hits"]
  erb :results
end
