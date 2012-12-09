#!/usr/bin/ruby

#require 'cakedb'
require './lib/cakedb.rb'

#@conn = Cake.new("localhost", 8888)
@conn = CakeDB.new
#@conn.write("a","cheeseburger")
#@conn.write("a","chips")
#@conn.write("a","softdrink")
#@conn.write("a","nachos")
#@conn.write("a","sushi")
includedTime = 0
@conn.allSince("a", 0).each do |line|
  puts line["ts"].to_s + "|" + line["data"]
end

@conn.rangeQuery("a", 0, Time.now.to_i*1000000).each do |line|
  puts line["ts"].to_s + "|" + line["data"]
  includedTime = line["ts"] * 10
end
@conn.lastAt("a", includedTime).each do |line|
  puts line["ts"].to_s + "|" + line["data"]
end

