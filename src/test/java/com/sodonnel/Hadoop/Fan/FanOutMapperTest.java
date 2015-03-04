package com.sodonnel.Hadoop.Fan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver; 
import org.junit.*;

public class FanOutMapperTest {

    MapDriver<LongWritable, Text, NullWritable, Text> mapDriver;
    
    @Before
    public void setup() {
        FanOutMapper mapper = new FanOutMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }
    
    @Test
    public void splitValidRecordIntoTokens() throws IOException, InterruptedException {
        List<Text> values = new ArrayList<Text>();
        values.add(new Text("abc"));
        mapDriver.withInput(new LongWritable(1), new Text("abcdef"))
    //    .withMultiOutput("a", NullWritable.get(), new Text("abcdef"))
        .withOutput(NullWritable.get(), new Text("abcdef"))
        .runTest();
    } 
    
}