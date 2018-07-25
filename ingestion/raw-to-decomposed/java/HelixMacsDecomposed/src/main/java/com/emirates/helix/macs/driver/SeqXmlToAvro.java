package com.emirates.helix.macs.driver;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Scanner;
import java.util.logging.Logger;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.emirates.helix.macs.*;
 

@SuppressWarnings("deprecation")
public class SeqXmlToAvro extends Configured implements Tool {

	//date format to append to output folder name 
	private static final SimpleDateFormat directoryFormat = new SimpleDateFormat("yyyy-MM-dd");

	// Mapper class
    private static class SeqXmlToAvroMapper extends Mapper<Text, Text, Object, NullWritable> {

    	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    		
        	
        	// Mapper to fetch xml, convert it to avro 
	    	DatumBuilder datumBuilder = new DatumBuilder(Schema.parse(context.getConfiguration().get("SCHEMASTRING")));
	        Object mydatum = datumBuilder.createDatum(value.toString());
	        AvroKey<GenericRecord> keyout = new AvroKey<GenericRecord>((GenericRecord) mydatum);
	        context.write(keyout, NullWritable.get());
		}
    }

    // Reducer class
    private static class IdentityAvroReducer extends Reducer<Object, NullWritable, Object, NullWritable> {
    	
    	public void reduce(Object key, Iterable<NullWritable> values, Context context) throws IOException,InterruptedException {
 	    	context.write( key, NullWritable.get());
	    }
	  }


    
  @Override
  public int run(String[] args) throws Exception {
 
/***********	  
        if (args.length != 2) {
        	System.out
          .printf("Two parameters need to be supplied - <input dir> and <output dir>\n");
          return -1;
       }
**********/	  
		// load schema of avro output
	  	Schema schema = null;
	    try {

	    	//Path pt=new Path("hdfs://dxbhqhdphn01.hq.emirates.com:8020/apps/DataLake/Development/test/test_akash/test.avsc");//Location of file in HDFS
	    	Path pt=new Path("hdfs://dxbhqhdphn01.hq.emirates.com:8020" + args[2]);//Location of file in HDFS
	    	FileSystem fs = FileSystem.get(new Configuration());
	    	BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
	    	StringBuffer doc = new StringBuffer();
	    	String line;
	    	line=br.readLine();
	    	while (line != null){
	    		System.out.println(line);
	    		doc.append(line);
	    		line=br.readLine();
            
	    	}
	    	Logger.getGlobal().info("FILE CONTENT IS --> "+ doc.toString());
	    	schema = Schema.parse(doc.toString()); 
	    }catch (final Exception exc) {
	        throw new Error(exc);
	    	//return null;
	    }


	  Configuration conf = new Configuration();
	  conf.set("mapred.child.java.opts", "-Xmx2048m -Xincgc");
  	  conf.set("SCHEMASTRING", schema.toString());
  	  
      Job job = new Job(conf);
      job.setJarByClass(SeqXmlToAvro.class);
      job.setJobName(" Job to Convert Sequence file to Avro file");
 
      FileInputFormat.addInputPath(job, new Path(args[0]));
      //FileInputFormat.addInputPath(job, new Path("/apps/DataLake/Development/test/test_akash/raw/"+ directoryFormat.format(new Date()) ) );
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      //FileOutputFormat.setOutputPath(job, new Path( "/apps/DataLake/Development/test/test_akash/decomposed/" + directoryFormat.format(new Date()) ) );
    
      job.setInputFormatClass(SequenceFileInputFormat.class);
      job.setOutputFormatClass(AvroKeyOutputFormat.class);
 
      job.setMapperClass(SeqXmlToAvroMapper.class);
      //job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(NullWritable.class);
      AvroJob.setMapOutputKeySchema(job, schema);
      AvroJob.setOutputKeySchema(job, schema);
      
      job.setReducerClass(IdentityAvroReducer.class);
      job.setNumReduceTasks(1);
      
      boolean success = job.waitForCompletion(true);
      return success ? 0 : 1;
   	}
 
    public static void main(String[] args) throws Exception {
    	
    	int exitCode = ToolRunner.run(new Configuration(), new SeqXmlToAvro(), args);
    	System.exit(exitCode);
    }
}