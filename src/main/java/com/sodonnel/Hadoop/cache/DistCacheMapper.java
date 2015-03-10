package com.sodonnel.Hadoop.cache;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

public class DistCacheMapper extends
    Mapper<LongWritable, Text, NullWritable, Text> {
    
    private static final Logger log = Logger.getLogger(DistCacheMapper.class);
    
    @Override
    public void setup(Context context)
            throws IOException, InterruptedException {
        // This is how to deal with the files in YARN / MapRed 2
        // if (context.getCacheFiles() != null
        //         && context.getCacheFiles().length > 0) {
        //     URI[] files = context.getCacheFiles();
        // Apparently the file can also be accessed by its identifier, ie
        // the part after the # in the filepath.
        // }
        Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        log.info(files[0].toString());
        
        BufferedReader reader = new BufferedReader(new FileReader(files[0].toString())); 
        String line;
        while ((line = reader.readLine())!= null) {
            log.info(line);    
        }

        super.setup(context);
    }    
    
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Throw away totally blank lines
        if (value.equals(new Text(""))) {
            return;
        }

        context.write(NullWritable.get(), value);
    }
}
