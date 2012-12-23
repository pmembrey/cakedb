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
			String cakeHost = "127.0.0.1";
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
			Thread.sleep(11000);

			//## driver.allSinceQuery()
			List<Event> events = driver.allSinceQuery(teststream, start); 			//get events
			assertEquals(1, events.size());
			
			//check payload
			String payload = new String(events.get(0).payload);
			assertEquals("Hello World", payload);
			
			//insert 99 more documents
			for(int a=0;a<99;a++){
				driver.store(teststream,"Hello World " + a);				
			}

			//wait a bit, because Cake doesnt flush straight away
			Thread.sleep(11000);

			//check we get 100 back
			events = driver.allSinceQuery(teststream,0);
			assertEquals(100,events.size());

			//simple query check - we want the items matching two timestamps
			//we will take 25th and 74th entry - 50 in total
			Event begin  = events.get(24);
			Event finish = events.get(73);

			//execute range query
			List<Event> range_events = driver.rangeQuery(teststream,begin.timestamp,finish.timestamp);

			//we should have 50 results and the start and end timestamps should match the 25th and 75th entry
			assertEquals(50,range_events.size());
			assertEquals(begin.timestamp,range_events.get(0).timestamp);
			assertEquals(finish.timestamp,range_events.get(49).timestamp);

			//entry at timestamp check - will take the final timestamp and incremement it
			//as there is no way to predict the time values during the test.
			Event last_at_event = driver.lastEntryAt(teststream,events.get(99).timestamp + 100000000);

			//check it has the timestamp (unique ID) of the last document we inserted
			assertEquals(last_at_event.timestamp,events.get(99).timestamp);

                        //make sure querying lastEntryAt with a very old timestamp returns null
                        //and does not crash the driver
                        last_at_event = driver.lastEntryAt(teststream,0);

                        assertNull(last_at_event);


		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		
	}

}
