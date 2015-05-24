package com.picsart.mariam.hadoop;

import org.apache.log4j.Logger;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;

/**
 * Created by mariam on 5/23/15.
 */
public class APILogParser {
    private static DateFormat apiTimeFormat = new SimpleDateFormat("d/MMM/yyyy:HH:m:s Z");
    private static final Logger LOG = Logger.getLogger(APILogParser.class);

    public UniformAPILogEntry parseEvent(String line) throws Exception{
        try {
            String[] splits = line.substring(0, line.lastIndexOf(" - ")).split(":");

            String hostName = splits[1].trim();
            Integer statusCode = Integer.parseInt(splits[2]);
            Integer responseSizeInBytes = !splits[3].equals("-") ? Integer.parseInt(splits[3]) : null;
            Float processingTime = !splits[4].equals("-") ? Float.parseFloat(splits[4]) : null;

            String ip = !splits[5].equals("-") ? splits[5] : null;
            String requestString = !splits[6].equals("-") ? splits[6] : null;
            String applicationInfo = !splits[7].equals("-") ? splits[7] : null;
            String deviceId = !splits[8].equals("-") ? splits[8] : null;
            String countryCode = !splits[9].equals("-") ? splits[9] : null;
            String languageCode = !splits[10].equals("-") ? splits[10] : null;
            String platform = !splits[11].equals("-") ? splits[11] : null;
            Integer version = !splits[12].equals("-") ? Integer.parseInt(splits[12]) : null;

            String datePart = line.substring(line.lastIndexOf("[") + 1, line.lastIndexOf("]"));
            LOG.debug("datePart = " + datePart);
            Long timeStamp = apiTimeFormat.parse(datePart).getTime();

            String reqType = line.substring(line.lastIndexOf("- ")).split(" ")[1];

            UniformAPILogEntry logEntry = new UniformAPILogEntry(reqType);
            logEntry.setHostName(hostName);
            logEntry.setStatusCode(statusCode);
            logEntry.setResponseSizeInBytes(responseSizeInBytes);
            logEntry.setProcessingTime(processingTime);
            logEntry.setIp(ip);
            logEntry.setRequestString(requestString);
            logEntry.setApplicationInfo(applicationInfo);
            logEntry.setDeviceId(deviceId);
            logEntry.setCountryCode(countryCode);
            logEntry.setLanguageCode(languageCode);
            logEntry.setPlatform(platform);
            logEntry.setAppVersion(version);
            logEntry.setRequestTime(timeStamp);

            return logEntry;
        } catch (Exception e) {
            LOG.error("Error in parser, line = " + line, e);
            throw new Exception(e);
        }
    }


}
