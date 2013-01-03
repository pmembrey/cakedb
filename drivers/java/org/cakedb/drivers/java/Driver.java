package org.cakedb.drivers.java;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Java driver for CakeDB
 */
public class Driver {

	//Cake server hostname/port
	private String serverHost = "";
	private int serverPort = 0;

	//socket & network streams
	private Socket clientSocket = null;
	private DataOutputStream outToServer = null;
	private DataInputStream inFromServer = null;

	//id map
	private HashMap<String, Short> streamIDs = new HashMap<String, Short>();
	
	//query types (see cake_protocol.erl)
	private static final int DB_NOOP = 0;
	private static final int DB_REQUEST_STREAM_WITH_SIZE = 1;
	private static final int DB_APPEND = 2;
	private static final int DB_QUERY = 3;
	private static final int DB_ALL_SINCE = 4;
	private static final int DB_REQUEST_STREAM = 5;
	private static final int DB_LAST_ENTRY_AT = 6;

	private static final int SIZE_SHORT = Short.SIZE/8;
	private static final int SIZE_INT = Integer.SIZE/8;
	private static final int SIZE_LONG = Long.SIZE/8;
	
	
	/** 
	 * Create a connection to the CakeDB server at this host/port.
	 * @param hostName
	 * @param port
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	public Driver(String hostName,int port) throws UnknownHostException, IOException {
		
		this.serverHost = hostName;
		this.serverPort = port;		
		
		reconnect();
	}
	
	/**
	 * Reconnect socket connection to the CakeDB server. Useful in case the socket
	 * connection fails for any reason.
	 */
	public void reconnect() throws UnknownHostException, IOException {
		
		// Try clean shutdown of old socket if needed
		if(clientSocket != null)
		{
			try {
				clientSocket.close();
			} catch(IOException e)
			{}
		}
		
		// Reconnect socket and related streams
		clientSocket = new Socket(serverHost, serverPort);
		outToServer  = new DataOutputStream(clientSocket.getOutputStream());
		inFromServer = new DataInputStream(clientSocket.getInputStream());
	}

	/**
	 * Helper method to store a string. Equivalent to 
	 * <pre>
	 * store(streamName, payload.getBytes());
	 * </pre> 
	 */
	public void store(String streamName,String payload) throws IOException {

		store(streamName, payload.getBytes());
	}

	/**
	 * Store data to CakeDB
	 * @param streamName
	 * @param payload
	 * @throws IOException
	 */
	public synchronized void store(String streamName,byte[] payload) throws IOException {
		
		short streamID = getStreamID(streamName); 	        // Get Stream ID
		int payloadLength = SIZE_SHORT + payload.length;  	// Calculate message length
		short op = DB_APPEND; 					            // Set operation		

		writeHeader(payloadLength, op);		
		outToServer.writeShort(streamID);
		outToServer.write(payload);
		outToServer.flush();
	}

	/**
	 * Standard header is a 4-byte contents length, followed by 2-byte message type  
	 * @throws IOException 
	 */
	private void writeHeader(int payloadLength, short operation) throws IOException {
		outToServer.writeInt(payloadLength);
		outToServer.writeShort(operation);
	}

	/**
	 * Retrieve all records between these two timestamps
	 * @param streamName
	 * @param from In UNIX epoch format e.g. that returned by Date().getTime();
	 * @param to In UNIX epoch format e.g. that returned by Date().getTime();
	 * @return
	 * @throws IOException
	 */
	public synchronized List<Event> rangeQuery(String streamName,long from, long to) throws IOException {
		
		short streamID = getStreamID(streamName);			// Get Stream ID
		short op = DB_QUERY;								

		//send query
		writeHeader(SIZE_SHORT + SIZE_LONG + SIZE_LONG, op);
		outToServer.writeShort(streamID);
		outToServer.writeLong(from);
		outToServer.writeLong(to);
		outToServer.flush();
		
		// Get the incoming stream length
		int length = inFromServer.readInt();	
				
		//create a buffer & read in rest of stream
		byte[] buf = new byte[length];
		inFromServer.readFully(buf);
		
		return extractData(buf);
	}

	/**
	 * Extract a list of events from a data buffer
	 */
	private List<Event> extractData(byte[] buf) {
		
		ArrayList<Event> list = new ArrayList<Event>();
		
		//iterate through buffer, pulling out events
		ByteBuffer x = ByteBuffer.wrap(buf);
		while(x.hasRemaining()){

			long timestamp = x.getLong();
			int recordLength = x.getInt();
			
			byte[] temp = new byte[recordLength];
			x.get(temp, 0, recordLength);
			
			Event event = new Event(timestamp,temp);
			list.add(event);			
		}
		
		return(list);
	}
	
	/**
	 * Return all events since this time
	 * @param streamName
	 * @param from In UNIX epoch format e.g. that returned by Date().getTime();
	 * @return
	 * @throws IOException
	 */
	public synchronized List<Event> allSinceQuery(String streamName,long from) throws IOException {
		
		short streamID = getStreamID(streamName);			// Get Stream ID
		short op = DB_ALL_SINCE;								

		//send query
		writeHeader(SIZE_SHORT + SIZE_LONG, op);
		outToServer.writeShort(streamID);
		outToServer.writeLong(from);
		outToServer.flush();

		// Get the incoming stream length
		int length = inFromServer.readInt();	
		
				
		//create a buffer & read in rest of stream
		byte[] buf = new byte[length];
		inFromServer.readFully(buf);
		
		return extractData(buf);

	}


		/**
	 * Return last event as of this time
	 * @param streamName
	 * @param at In UNIX epoch format e.g. that returned by Date().getTime();
	 * @return
	 * @throws IOException
	 */
	public synchronized Event lastEntryAt(String streamName,long at) throws IOException {
		
		short streamID = getStreamID(streamName);			// Get Stream ID
		short op = DB_LAST_ENTRY_AT;								

		//send query
		writeHeader(SIZE_SHORT + SIZE_LONG, op);
		outToServer.writeShort(streamID);
		outToServer.writeLong(at);
		outToServer.flush();

		// Get the incoming stream length
		int length = inFromServer.readInt();	
		
				
		//create a buffer & read in rest of stream
		byte[] buf = new byte[length];
		inFromServer.readFully(buf);

                List<Event> data = extractData(buf);
		
		return (data.size() > 0) ? data.get(0) : null;

	}

	/**
	 * Get the stream ID. Check the cache if we have it, and if we dont, then
	 * request from the server.
	 */
	private short getStreamID(String streamName) throws IOException {

		// Try to fetch ID from HashMap
		Short streamID = streamIDs.get(streamName);

		// If we haven't got an ID for this stream, request it from CakeDB
		if(streamID == null){
			streamID = getStreamIDFromServer(streamName);
			streamIDs.put(streamName, streamID);   			// Stick it in the HashMap for later
		}

		return(streamID);
	}


	/**
	 * Get stream ID from Cake server. Send the stream name, and expect a 16bit short in return.
	 */
	private short getStreamIDFromServer(String streamName) throws IOException {

		int payloadLength = streamName.length();				// Calculate length of message
		short op = DB_REQUEST_STREAM;		

		//send request for this stream name
		writeHeader(payloadLength, op);		
		outToServer.write(streamName.getBytes());
		outToServer.flush();

		return inFromServer.readShort();
	}
}
