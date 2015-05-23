package com.picsart.mariam.hadoop;

import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.text.SimpleDateFormat;

import static org.junit.Assert.*;

/**
 * Created by william on 5/23/15.
 */
public class UniformAPILogEntryTest {
    UniformAPILogEntry uniformAPILogEntry;
    DataOutputStream outputStream;
    DataInputStream inputStream;

    @Before
    public void setUp() throws Exception {
        uniformAPILogEntry = new APILogParser().parseEvent("nginx: dal-front2:200:57:0.000:-:/notifications/show/me.json?par1=val1:Picsart/3.0:andy-4d50258d-7b5a-44fc-971a-eb9a203fc524:TR:tr:android:141 - GET 200 57 [23/May/2015:06:39:48 +0000]");
        //outputStream = new DataOutputStream(new FileOutputStream("test"));
        inputStream = new DataInputStream(new FileInputStream("test"));
    }

    @Test
    public void testWrite() throws Exception {
        uniformAPILogEntry.write(outputStream);
    }

    @Test
    public void testReadFields() throws Exception{
        UniformAPILogEntry logEntry = new UniformAPILogEntry("GET");
        logEntry.readFields(inputStream);

        assertEquals("dal-front2", uniformAPILogEntry.getHostName());
        assertEquals(new Integer(200), uniformAPILogEntry.getStatusCode());
        assertEquals(new Integer(57), uniformAPILogEntry.getResponseSizeInBytes());
        assertEquals(new Float(0.000), uniformAPILogEntry.getProcessingTime());
        assertEquals("/notifications/show/me.json?par1=val1", uniformAPILogEntry.getRequestString());
        assertEquals("/notifications/show/me.json", uniformAPILogEntry.getRequestURL());
        assertEquals("andy-4d50258d-7b5a-44fc-971a-eb9a203fc524", uniformAPILogEntry.getDeviceId());
        assertEquals(new Integer(141), uniformAPILogEntry.getAppVersion());
        assertEquals("GET", uniformAPILogEntry.getReqType());
        assertEquals(new SimpleDateFormat("d/MMM/yyyy:HH:m:s Z").parse("23/May/2015:06:39:48 +0000").getTime(), uniformAPILogEntry.getRequestTime().longValue());

    }
}