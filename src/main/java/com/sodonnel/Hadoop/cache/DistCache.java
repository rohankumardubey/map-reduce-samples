package com.sodonnel.Hadoop.cache;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.sodonnel.Hadoop.Fan.Fan;
import com.sodonnel.Hadoop.Fan.FanOutMapper;

public class DistCache {

    public int run(String[] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);

        // Create configuration
        Configuration conf = new Configuration(true);

        // Create job
        Job job = Job.getInstance(conf, "DistCache");
        job.setJarByClass(getClass());
        
        // This is the MapRed V1 way to add cache files
        // Under YARN you should use
        //    job.addCacheFile(uri);
        //
        // Where Uri can take a path like /path/to/file#identifier - the identifier
        // part can be used to retrieve the file in the mapper
        //    File some_file = new File("./identifier");
        // I have not tested this method out.
        DistributedCache.addCacheFile(new Path("./input/data.csv").toUri(), conf);
        
        // Setup MapReduce
        job.setMapperClass(DistCacheMapper.class);
        job.setNumReduceTasks(0);

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, outputDir);

        // Delete output if exists
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);

        // Execute job
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Fan(), args);
        System.exit(exitCode);
    }
    
}
