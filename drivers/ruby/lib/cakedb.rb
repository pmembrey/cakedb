#!/usr/bin/ruby

require 'socket'

class CakeDB
  def initialize( server="127.0.0.1", port=8888 )
    #puts "Connection Initiated"
    @server = TCPSocket.new(server, port)
    @sids = {}
  end
  def get_sid(stream)
    if @sids[stream].nil?
      @server.write [stream.bytesize,5].pack("L>S>")  
      @server.write stream  
      @server.flush
      sid = @server.recv(2).unpack("S>")
      @sids[stream] = sid[0]
      return sid[0]
    else
      return @sids[stream]
    end
  end

  def write(stream,payload)
    sid = get_sid(stream)
    #puts sid
    @server.write [(payload.bytesize + 2),2,sid].pack("L>S>S>")
    @server.write payload
    @server.flush
  end

  def get_tasking(msg)
    details = Hash.new
    details["database"] = msg["region"]
    details["task"]     = msg["tasking"]
    details["date"]     = msg["date"]
    details["rediskey"] = details["database"] + "-" + details["task"]
    return details
  end

  def read(stream, ts, tsend=nil, mode=4)
    #Issue the query
    sid = get_sid(stream)
    if tsend 
      @server.write [18,mode,sid,ts,tsend].pack("L>S>S>Q>Q>")
    else
      @server.write [10,mode,sid,ts].pack("L>S>S>Q>")
    end
    #@server.write [headersz,mode,sid,ts].pack("L>S>S>Q>") 
    @server.flush
    result = Array.new

    #How much data we gotta get?
    recv_total = @server.recv(4).unpack("L>")[0]
    #puts "total is #{recv_total}"
    if recv_total > 0
      counter = 0
      #Recive the result
      while recv_total > 0
	#Get the header - 12 bytes
	header_whole = @server.recv(12)
	header = header_whole.unpack("Q>L>")
	if header[0] == nil || header[1] == nil
	  puts "You have been eaten by a Grue"
	end
	if header_whole.bytesize !=12
	  puts "Incomplete header"
	  puts "Length: #{header[1]}"
	  puts "TS: #{header[0]}"
	end

	#recieve the data, header says how much
	result[counter] = Hash.new
	result[counter]["ts"] = header[0]
        result[counter]["data"] = @server.recv(header[1])
	
	#Keep reading till we have everything we expect
        while(result[counter]["data"].bytesize < header[1])
          result[counter]["data"] += @server.recv(header[1] - result[counter]["data"].bytesize)
          puts "Incomplete read: #{result[counter]["data"].bytesize} / #{header[1]}"
	end
	if result[counter]["data"].bytesize != header[1]
          puts "Incomplete read on payload receive"
	  puts "Raw Data: #{resultt[counter]}"  
	  puts "Length: #{header[1]} bytes"
	  puts "Raw Data length: #{resultt[counter].bytesize - 12}"
	  puts "Total Length: #{recv_total}"
	  exit(1)
        end
	recv_total = recv_total - header[1] - 12
	counter+=1
      end
      return result
    else
      # I SEE NOTHING!
    end
  end

  def allSince(stream, ts)
    #allSince is mode 4
    return read(stream, ts, nil, 4)
  end

  def lastAt(stream, ts)
    #lastAt is mode 6
    return read(stream, ts, nil, 6)
  end
    #Perform a range query
  def rangeQuery(stream, ts, tsend)
    return read(stream, ts, tsend, 3)
  end
end
