package com.picsart.mariam.hadoop;

import org.junit.Before;
import org.junit.Test;

import java.text.SimpleDateFormat;

import static org.junit.Assert.*;

/**
 * Created by mariam on 5/23/15.
 */
public class APILogParserTest {

    String line;

    @Before
    public void setUp() throws Exception {
        line = "nginx: dal-front2:200:57:0.000:-:/notifications/show/1234.json?par1=val1:Picsart/3.0:andy-4d50258d-7b5a-44fc-971a-eb9a203fc524:TR:tr:android:141 - GET 200 57 [23/May/2015:06:39:48 +0000]";
    }

    @Test
    public void testParseEvent() throws Exception {
        UniformAPILogEntry uniformAPILogEntry = new APILogParser().parseEvent(line) ;

        assertEquals("dal-front2", uniformAPILogEntry.getHostName());
        assertEquals(new Integer(200), uniformAPILogEntry.getStatusCode());
        assertEquals(new Integer(57), uniformAPILogEntry.getResponseSizeInBytes());
        assertEquals(new Float(0.000), uniformAPILogEntry.getProcessingTime());
       // assertEquals("/notifications/show/me.json?par1=val1", uniformAPILogEntry.getRequestString());
        assertEquals("/notifications/show.json", uniformAPILogEntry.getRequestURL().replaceAll("[0-9]+.json", ""));
        assertEquals("andy-4d50258d-7b5a-44fc-971a-eb9a203fc524", uniformAPILogEntry.getDeviceId());
        assertEquals(new Integer(141), uniformAPILogEntry.getAppVersion());
        assertEquals("GET", uniformAPILogEntry.getReqType());
        assertEquals(new SimpleDateFormat("d/MMM/yyyy:HH:m:s Z").parse("23/May/2015:06:39:48 +0000").getTime(), uniformAPILogEntry.getRequestTime().longValue());
    }
}