package com.sodonnel.Hadoop.Fan;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

public class FanOutMapper extends
    Mapper<Object, Text, Text, Text> {
    
    private static Logger logger = Logger.getLogger(FanOutMapper.class.getName());

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        // Throw away totally blank lines
        if (value.equals(new Text(""))) {
            return;
        }

        // get the first character of the string and use it as the key
        // this will be used as the filename later
        logger.info(value);
        String keyChar = value.toString().substring(0,1).toLowerCase();
        logger.info(keyChar);
        context.write(new Text(keyChar), value);
    }
}