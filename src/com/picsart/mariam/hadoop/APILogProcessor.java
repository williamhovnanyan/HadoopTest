package com.picsart.mariam.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
        private static int counter = 0;


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {

                UniformAPILogEntry logEntry = logParser.parseEvent(value.toString());

                Text statusCodePerRequestKey = getStatusCodePerRequestKey(logEntry);
                Text avgProcessingTimePerRequestKey = getAVGProcessingTimePerURLKey(logEntry);

                if(++counter % 100 == 0) {
                    LOG.info("Log is " + value.toString());
                    LOG.info("statusCodePerRequestKey = " + statusCodePerRequestKey);
                    LOG.info("avgProcessingTimePerRequestKey = " + avgProcessingTimePerRequestKey);
                }
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
            String reqURL = logEntry.getRequestURL() != null ? logEntry.getRequestURL().replaceAll("[0-9]+.json", "") : UNDEFINED;

            return new Text(PROCESSING_KEYS.STATUS_CODE_PER_REQUEST + DELIMITER + reqURL);
        }
    }

    private static class LogRecuder extends Reducer<Text, UniformAPILogEntry, Text, Text> {
        private static final Logger LOG = Logger.getLogger(LogRecuder.class);
        private static final String DELIMITER = "::";

        @Override
        protected void reduce(Text key, Iterable<UniformAPILogEntry> values, Context context) throws IOException, InterruptedException {
            try {
                LOG.info("key = " + key.toString());

                String[] keys = key.toString().split(DELIMITER);
                String processingType = keys[0];

                if(processingType.equals(PROCESSING_KEYS.AVERAGE_PROCESSING_TIME_PER_REQUEST.toString())) {
                    calculateAverageProcessingTimePerRequest(keys[1], values, context);
                }

                if(processingType.equals(PROCESSING_KEYS.STATUS_CODE_PER_REQUEST.toString())) {
                    calculateStatusCodePerRequest(keys[1], values, context);
                }

            } catch (Exception e) {
                LOG.error("Error in reduce", e);
            }
        }

        private void calculateStatusCodePerRequest(String reqURL, Iterable<UniformAPILogEntry> values, Context context) throws IOException, InterruptedException {
            LOG.info("In calculateStatusCodePerRequest, url = " + reqURL);
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
            LOG.info("In calculateAverageProcessingTimePerRequest, url = " + reqURL);
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

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LOG processor");
        job.setJarByClass(APILogProcessor.class);
        job.setJarByClass(APILogParser.class);
        job.setJarByClass(UniformAPILogEntry.class);
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogRecuder.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(UniformAPILogEntry.class);

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatuses = fs.listStatus(new Path(args[0]));

        for (FileStatus fileStatus : fileStatuses) {
            FileInputFormat.addInputPath(job, fileStatus.getPath());
        }

        fs.delete(new Path(args[1]), true);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
