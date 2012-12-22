package org.cakedb.drivers.java;

/**
 * Holds event data from Cake. This consists of a timestamp & payload pair.
 */
public class Event {
    
    public long timestamp = 0;
    public byte[] payload = null;
    
    public Event(long timestamp,byte[] payload) {
        
        this.timestamp = timestamp;
        this.payload = payload;
    }
    
    public  String toString() {
    
        String payload = new String(this.payload);
        return "{Timestamp: " + this.timestamp
            + ", Payload: " + payload + "}";
    }
}
