package com.sodonnel.Hadoop.Fan;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

public class FanOutReducer extends
    Reducer<Text, Text, NullWritable, Text> {
    
    private static Logger logger = Logger.getLogger(FanOutReducer.class.getName());
    
    private MultipleOutputs<NullWritable, Text> mos;
    
    @Override
    public void setup(Context context) {
       // mos = new MultipleOutputs<Text, Text>(context);
        mos = new MultipleOutputs<NullWritable, Text>(context);
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
    
    public void reduce(Text text, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        logger.info(text.toString());
        for (Text value : values) {
            mos.write(NullWritable.get(), value, text.toString());
            mos.write("text1", NullWritable.get(), value);
            mos.write("text2", NullWritable.get(), value);
            context.write(NullWritable.get(), value);
        }
    }
}