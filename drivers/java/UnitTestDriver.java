package org.cakedb.drivers.java;

import static org.junit.Assert.*;

import java.util.Date;
import java.util.List;

import org.junit.Test;

public class UnitTestDriver {

	@Test
	public void test() {
		
		Driver driver;
		try {

			//Cake server to test
			String cakeHost = "10.2.43.145";
			int cakePort = 8888;
			
			//get time from a while back, in case clocks arent quite in sync
			long start = new Date().getTime() - (60 * 60 * 1000);
			
			//generate random stream name
			String teststream = "teststream" + start;
			
			//connect
			driver = new Driver(cakeHost, cakePort);
			
			//## driver.store()
			driver.store(teststream, "Hello World");

			//wait a bit, because Cake doesnt flush straight away
			Thread.sleep(20000);

			//## driver.allSinceQuery()
			List<Event> events = driver.allSinceQuery(teststream, start); 			//get events
			assertEquals(1, events.size());
			
			//check payload
			String payload = new String(events.get(0).payload);
			assertEquals("Hello World", payload);
			
			
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		
	}

}
