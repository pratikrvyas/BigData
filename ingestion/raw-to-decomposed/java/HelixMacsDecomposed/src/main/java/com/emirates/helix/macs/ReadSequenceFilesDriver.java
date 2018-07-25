package com.emirates.helix.macs;



import java.net.URI;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 
public class ReadSequenceFilesDriver extends Configured implements Tool {
 
  @Override
  public int run(String[] args) throws Exception {
 
/*    if (args.length != 2) {
      System.out
          .printf("Two parameters need to be supplied - <input dir> and <output dir>\n");
      return -1;
    }*/
	  
		Configuration conf = new Configuration();
		conf.set("mapred.child.java.opts", "-Xmx2048m -Xincgc");
	  DistributedCache.addCacheFile(new URI("/OALMIDTTOP3/part-m-00000#PAXXSD"),conf);
	  DistributedCache.addCacheFile(new URI("/OALMIDTTOP3/part-m-00000#FLTXSD"),conf);
		DistributedCache.createSymlink(conf);
 
    Job job = new Job(conf);
    job.setJarByClass(ReadSequenceFilesDriver.class);
    job.setJobName("Convert Sequence File and Output as Text");
 
 //   FileInputFormat.setInputPaths(job, new Path("/apps/DataLake/Development/test/seqfile"));
    //FileInputFormat.setInputPaths(job, new Path("/apps/DataLake/Development/test/saqfile1"));
    FileInputFormat.setInputPaths(job, new Path("/apps/DataLake/Development/test/wholefileSeq"));
    
    FileOutputFormat.setOutputPath(job, new Path("/apps/DataLake/Development/test/wholefileseqsplitter"));
 
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapperClass(SequenceMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
   job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
   job.setReducerClass(SequenceReducer.class);
   AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
	
    job.setNumReduceTasks(10);
 
    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }
 
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new ReadSequenceFilesDriver(), args);
    System.exit(exitCode);
  }
}