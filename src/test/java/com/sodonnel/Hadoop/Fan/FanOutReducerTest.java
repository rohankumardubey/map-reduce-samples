package com.sodonnel.Hadoop.Fan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver; 
import org.junit.*;

public class FanOutReducerTest {

    ReduceDriver<Text, Text, NullWritable, Text> reduceDriver;
    
    @Before
    public void setup() {
        FanOutReducer reducer = new FanOutReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }
    
    @Test
    public void splitValidRecordIntoTokens() throws IOException, InterruptedException {
        List<Text> values = new ArrayList<Text>();
        values.add(new Text("abc"));
        reduceDriver.withInput(new Text("a"), values)
        //.withOutput(null, new Text("abc"))
        .withMultiOutput("a", NullWritable.get(), new Text("abc"))
        .runTest();
    } 
    
}
