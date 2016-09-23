# Usage:
# 
#       $ ruby import.rb /path/o/file.csv

require 'tiny_tds'
require 'activerecord-sqlserver-adapter'
require 'tire'
require 'csv'
require 'mail'


batch_size = 100
buffer     = []
index_name = ARGV[0]


class Newstar < ActiveRecord::Base
  self.primary_key = 'RowID'

  self.pluralize_table_names = false
  self.table_name_prefix = 'dbo.'

  self.establish_connection(
    :adapter    => "sqlserver",
    :host       => "bhmnewstar.signature.local",
    :database   => "informXL_dm",
    :username   => "analyzer",
    :password   => "xxx",
    :timeout    => "30000",
    :persistent => "true"
  )
end


begin
  tries ||= 6
  parts = Newstar.connection.select_all("SELECT ppc.[catdesc] as catdesc
      ,ppsc.[subcatdesc] as subcatdesc
      ,pp.[partcode] as partcode
      ,pp.[partdesc] as partdesc
      ,pp.[mfgname] as mfgname
      ,pp.[catalogcode] as catalogcode
  FROM [HBLive].[rems].[pupart] as pp
   INNER JOIN [HBLive].[rems].[pupartcat] as ppc
    ON pp.[catcode] = ppc.[catcode] AND pp.[partlist] = ppc.[partlist]
   INNER JOIN [HBLive].[rems].[pupartsubcat] as ppsc
    ON pp.[subcatcode] = ppsc.[subcatcode] AND pp.[partlist] = ppsc.[partlist]
  WHERE pp.[partlist] = 'BHAM'
  AND pp.[discontinued] = 0
  ORDER BY pp.[partcode]")
rescue => exception
  if (tries -= 1) > 0
    sleep 300
    retry
  else
    email = Mail.new do
      from      'chris@e-signaturehomes.com'
      to        'chris@e-signaturehomes.com'
      subject   "Failure: Parts Import Script for parts-search.e-signaturehomes.com: #{index_name}"
      body      exception.backtrace
    end
    email.deliver!
    abort
  end
end


if parts.count > 0
  Tire.index index_name.downcase do
    delete
  end 
end

parts.each do |p|

  part = Hash.new
  part["catdesc"] = p['catdesc']
  part["subcatdesc"] = p['subcatdesc']
  part["partcode"] = p['partcode']
  part["partdesc"] = p['partdesc']
  part["mfgname"] = p['mfgname']
  part["catalogcode"] = p['catalogcode']

  # Add a line as JSON buffer
  #
  buffer << part

  # When we hit the match boundary...
  if buffer.size % batch_size == 0

    # ... load batch inot Elasticsearch...
    Tire.index index_name.downcase do
      STDERR.puts import(buffer), '-'*80
    end

    # ... and empty buffer
    buffer = []
  end
end

# Import any rest
#
Tire.index index_name.downcase do
  STDERR.puts import(buffer), '-'*80
end unless buffer.empty?
