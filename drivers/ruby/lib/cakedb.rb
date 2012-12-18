#!/usr/bin/ruby

require 'socket'

class CakeDB
  def initialize( server="127.0.0.1", port=8888, lvl=0 )
    #puts "Connection Initiated"
    @server = TCPSocket.new(server, port)
    @sids = {}
    @loggingLevel = lvl
    @timeout = 30
  end
  def get_sid(stream)
    if @sids[stream].nil?
      @server.write [stream.bytesize,5].pack("L>S>")  
      @server.write stream  
      @server.flush
      sid = data_in(2).unpack("S>")
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

  def data_in(length)
    data = @server.recv(length)
    got = data.bytesize
    timeOver = Time.now + @timeout
    while got < length
      data += @server.recv(length-got)
      got = data.bytesize
      if Time.now > timeOver
        raise "Timeout on incomplete read, returning what I had"
	if @loggingLevel == 1
          puts "Timeout Hit. GT #{timeOver}"
	end
        return data
      end
      if @loggingLevel == 2 && got < length
        puts "Incomplete read on payload receive"
        puts "Raw Data: #{data}"
        puts "Length: #{length} bytes"
        puts "Raw Data length: #{data.bytesize}"
      end
    end
    return data
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
    recv_total = data_in(4).unpack("L>")[0]
    #puts "total is #{recv_total}"
    if recv_total > 0
      counter = 0
      #Recive the result
      while recv_total > 0
	#Get the header - 12 bytes
	header_whole = data_in(12)
	header = header_whole.unpack("Q>L>")

	#recieve the data, header says how much
	result[counter] = Hash.new
	result[counter]["ts"] = header[0]
        result[counter]["data"] = data_in(header[1])
	recv_total = recv_total - header[1] - 12
	counter+=1
      end
    else
      # I SEE NOTHING!
    end
    return result
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
