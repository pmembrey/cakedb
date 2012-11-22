package org.cakedb.drivers.java;

public class Event {
    
    public int length = 0;
    public long timestamp = 0;
    public byte[] payload = null;
    
    public Event(long timestamp,int length,byte[] payload){
        
        this.length = length;
        this.timestamp = timestamp;
        this.payload = payload;
        
        
    }

}
