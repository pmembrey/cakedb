package org.cakedb.drivers.java;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;

public class Driver {

	Socket clientSocket = null;
	OutputStream outToServer = null;
	InputStream inFromServer = null;
	HashMap<String, Short> streamIDs = new HashMap<String, Short>();



	public Driver(String hostName,int port) throws Exception{

		// Set up socket and related streams
		clientSocket = new Socket(hostName, port);
		outToServer  = clientSocket.getOutputStream();
		inFromServer = clientSocket.getInputStream();
	}

	public void storeString(String streamName,String payload) throws IOException {

		store(streamName,payload.getBytes());

	}

	public synchronized void store(String streamName,byte[] payload) throws IOException{

		// Allocate buffer for header
		ByteBuffer storeBuffer = ByteBuffer.allocate(8);
		// Get Stream ID
		short streamID = getStreamID(streamName);
		// Calculate message length
		int payloadLength = payload.length + 2;
		// Set type to STORE
		short type = 2;		
		// Load header into the byte buffer
		storeBuffer.putInt(payloadLength);
		storeBuffer.putShort(type);
		storeBuffer.putShort(streamID);
		// Write header and payload to socket
		outToServer.write(storeBuffer.array());
		outToServer.write(payload);


	
	}

	
	
	public synchronized ArrayList<Event> rangeQuery(String streamName,long from, long to) throws IOException{
		
		//Allocate buffer
		ByteBuffer storeBuffer = ByteBuffer.allocate(24);
		// Get Stream ID
		short streamID = getStreamID(streamName);
		// Set type to RANGE_QUERY
		short type = 3;
		storeBuffer.putInt(18);
		storeBuffer.putShort(type);
		storeBuffer.putShort(streamID);
		storeBuffer.putLong(from);
		storeBuffer.putLong(to);
		outToServer.write(storeBuffer.array());
		
		// Need the length
		
		// Allocate buffer for reply
		ByteBuffer reply = ByteBuffer.allocate(4);
		// Read the two byte reply
		reply.put((byte)inFromServer.read());
		reply.put((byte)inFromServer.read());
		reply.put((byte)inFromServer.read());
		reply.put((byte)inFromServer.read());
		// Rewind buffer
		reply.rewind();
		// Read off the short
		int length = reply.getInt();
		System.out.println("Length: " + length + "\n");
		
		byte[] buf = new byte[length];
		int offset = 0;
		int remainingLength = length;
		
		while(remainingLength >0){
		
		int in = inFromServer.read(buf, offset, remainingLength);
		offset = offset + in;
		remainingLength = remainingLength - in;
		}
		
		ArrayList<Event> list = new ArrayList<Event>();
		
		// Get first length 
		ByteBuffer x = ByteBuffer.wrap(buf);
		int counter = 0;
		while(x.hasRemaining()){
		counter++;
		
		long timestamp = x.getLong();
		int firstLength = x.getInt();

		
		byte[] temp = new byte[firstLength];
		x.get(temp, 0, firstLength);
		
		Event event = new Event(timestamp,length,temp);
		list.add(event);

		
			
		}
		
		System.out.println("Total count: " + counter);
		return(list);
	}
	
	
	
	public synchronized ArrayList<Event> allSinceQuery(String streamName,long from) throws IOException{
		
		//Allocate buffer
		ByteBuffer storeBuffer = ByteBuffer.allocate(16);
		// Get Stream ID
		short streamID = getStreamID(streamName);
		// Set type to ALL_SINCE_QUERY
		short type = 4;
		storeBuffer.putInt(10);
		storeBuffer.putShort(type);
		storeBuffer.putShort(streamID);
		storeBuffer.putLong(from);
		outToServer.write(storeBuffer.array());
		
		// Need the length
		
		// Allocate buffer for reply
		ByteBuffer reply = ByteBuffer.allocate(4);
		// Read the two byte reply
		reply.put((byte)inFromServer.read());
		reply.put((byte)inFromServer.read());
		reply.put((byte)inFromServer.read());
		reply.put((byte)inFromServer.read());
		// Rewind buffer
		reply.rewind();
		// Read off the short
		int length = reply.getInt();
		//System.out.println("Length: " + length + "\n");
		
		byte[] buf = new byte[length];
		int offset = 0;
		int remainingLength = length;
		
		while(remainingLength >0){
		
		int in = inFromServer.read(buf, offset, remainingLength);
		offset = offset + in;
		remainingLength = remainingLength - in;
		}
		
		ArrayList<Event> list = new ArrayList<Event>();
		
		// Get first length 
		ByteBuffer x = ByteBuffer.wrap(buf);
		int counter = 0;
		while(x.hasRemaining()){
		counter++;

		
		long timestamp = x.getLong();
		int firstLength = x.getInt();
		
		byte[] temp = new byte[firstLength];
		x.get(temp, 0, firstLength);
		
		Event event = new Event(timestamp,length,temp);
		list.add(event);
				
			
		}
		
	
		return(list);
	}

	private short getStreamID(String streamName) throws IOException  {

		// Define streamID
		short streamID;
		// Try to fetch ID from HashMap
		Object x = streamIDs.get(streamName);
		// What did we get back?
		if(x==null){
			// Haven't got an ID for this stream so request from CakeDB
			streamID = getStreamIDFromServer(streamName);
			// Stick it in the HashMap for later
			streamIDs.put(streamName, streamID);
		}
		else{
			// Cast the ID back to a short
			streamID = (Short)x;

		}

		return(streamID);


	}


	private short getStreamIDFromServer(String streamName) throws IOException {

		// Calculate length of message
		int payloadLength = streamName.length();
		// Set type to REQUEST (1)
		short type = 5;		
		// Allocate buffer for header
		ByteBuffer header = ByteBuffer.allocate(6);
		// Insert header data
		header.putInt(payloadLength);
		header.putShort(type);
		// Write to socket
		outToServer.write(header.array());
		outToServer.write(streamName.getBytes());
		// Allocate buffer for reply
		ByteBuffer reply = ByteBuffer.allocate(2);
		// Read the two byte reply
		reply.put((byte)inFromServer.read());
		reply.put((byte)inFromServer.read());
		// Rewind buffer
		reply.rewind();
		// Read off the short
		return reply.getShort();

	}
}