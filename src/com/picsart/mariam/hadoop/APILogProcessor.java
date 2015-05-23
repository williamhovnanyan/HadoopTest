package com.picsart.mariam.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by mariam on 5/23/15.
 */
public class APILogProcessor {

    enum PROCESSING_KEYS {
        STATUS_CODE_PER_REQUEST("statusCodePerRequest"),
        AVERAGE_PROCESSING_TIME_PER_REQUEST("avgProcessingTimePerRequest");

        private String type;

        PROCESSING_KEYS(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }
    }

    private static class LogMapper extends Mapper<LongWritable, Text, Text, UniformAPILogEntry> {
        private static final Logger LOG = Logger.getLogger(LogMapper.class);
        private static final APILogParser logParser = new APILogParser();
        private static final String DELIMITER = "::";
        private static final String UNDEFINED = "unknown" ;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                UniformAPILogEntry logEntry = logParser.parseEvent(value.toString());

                Text statusCodePerRequestKey = getStatusCodePerRequestKey(logEntry);
                Text avgProcessingTimePerRequestKey = getAVGProcessingTimePerURLKey(logEntry);

                context.write(statusCodePerRequestKey, logEntry);
                context.write(avgProcessingTimePerRequestKey, logEntry);
            } catch (Exception e) {
                LOG.error("Exception in Mapper", e);
            }
        }

        private Text getAVGProcessingTimePerURLKey(UniformAPILogEntry logEntry) {
            String reqURL = logEntry.getRequestURL() != null ? logEntry.getRequestURL() : UNDEFINED;

            return new Text(PROCESSING_KEYS.AVERAGE_PROCESSING_TIME_PER_REQUEST + DELIMITER + reqURL);
        }

        private Text getStatusCodePerRequestKey(UniformAPILogEntry logEntry) {
            String reqURL = logEntry.getRequestURL() != null ? logEntry.getRequestURL() : UNDEFINED;

            return new Text(PROCESSING_KEYS.STATUS_CODE_PER_REQUEST + DELIMITER + reqURL);
        }
    }

    private static class LogRecuder extends Reducer<Text, UniformAPILogEntry, Text, Text> {
        private static final Logger LOG = Logger.getLogger(LogRecuder.class);
        private static final String DELIMITER = "::";

        @Override
        protected void reduce(Text key, Iterable<UniformAPILogEntry> values, Context context) throws IOException, InterruptedException {
            try {
                String[] keys = key.toString().split(DELIMITER);
                String processingType = keys[0];

                if(processingType.equals(PROCESSING_KEYS.AVERAGE_PROCESSING_TIME_PER_REQUEST.getType())) {
                    calculateAverageProcessingTimePerRequest(keys[1], values, context);
                }

                if(processingType.equals(PROCESSING_KEYS.STATUS_CODE_PER_REQUEST.getType())) {
                    calculateStatusCodePerRequest(keys[1], values, context);
                }

            } catch (Exception e) {
                LOG.error("Error in reduce", e);
            }
        }

        private void calculateStatusCodePerRequest(String reqURL, Iterable<UniformAPILogEntry> values, Context context) throws IOException, InterruptedException {
            Iterator<UniformAPILogEntry> it = values.iterator();

            Integer count = 0;
            while (it.hasNext()) {
                it.next();
                count++;
            }
            context.write(new Text(PROCESSING_KEYS.STATUS_CODE_PER_REQUEST.toString() + DELIMITER + reqURL),
                    new Text(count.toString()));
        }

        private void calculateAverageProcessingTimePerRequest(String reqURL, Iterable<UniformAPILogEntry> values, Context context) throws IOException, InterruptedException {
            Iterator<UniformAPILogEntry> it = values.iterator();

            Double sumTime = 0.0;
            Integer count = 0;
            while (it.hasNext()) {
                UniformAPILogEntry logEntry = it.next();
                sumTime += logEntry.getProcessingTime();
                count++;
            }

            context.write(new Text(PROCESSING_KEYS.AVERAGE_PROCESSING_TIME_PER_REQUEST.toString() + DELIMITER + reqURL),
                    new Text(String.valueOf(sumTime/count)));
        }
    }
}
