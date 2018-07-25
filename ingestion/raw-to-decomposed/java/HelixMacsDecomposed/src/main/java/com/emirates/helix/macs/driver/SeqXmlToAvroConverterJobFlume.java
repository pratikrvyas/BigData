package com.emirates.helix.macs.driver;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Scanner;
import java.util.logging.Logger;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import com.emirates.helix.macs.mrutils.InputSplitUtils; 
import com.emirates.helix.macs.customIOformat.CustomAvroKeyOutputFormat;
import com.emirates.helix.macs.customIOformat.MyCustomInputFormat;

@SuppressWarnings("deprecation")
public class SeqXmlToAvroConverterJobFlume extends Configured implements Tool {
 
	// load schema of avro output 
	public static final Schema schema = createSchema();
	private static Schema createSchema() {
	    try {
	    	String content = new Scanner(new File("test.avsc")).useDelimiter("\\Z").next();
	        return Schema.parse(content);
	    } catch (final Exception exc) {
	        throw new Error(exc);
	    }  
	}

	//date format to append to output folder name 
	private static final SimpleDateFormat directoryFormat = new SimpleDateFormat("yyyy-MM-dd");

	// Mapper class
    private static class SeqXmlToSeqAvroMapper extends Mapper<Text, Text, Text, Text> {

    	// override to setup to persist the processed splits info 
    	@Override
    	protected void setup(Context context) throws IOException, InterruptedException {
    		String splitId = InputSplitUtils.getInputSplitId(context.getInputSplit());
    		InputSplitUtils.saveInputSplitId(context, splitId);
    	}
    	// Identity mapper to send data to reducer	
    	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    		
    		context.write(key, value);
		}
    }
  
    // Reducer class
    private static class SeqXmlToSeqAvroReducer extends Reducer<Text, Text, Object, NullWritable> {
    	// Create setup method to read schema once from conf
    	
    	// Reducer to fetch xml, convert it to avro and append to a single avro output file
    	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException {

		    StringBuffer xml =  new StringBuffer();
	 	    Iterator<Text> textIterator = values.iterator();
	    	while (textIterator.hasNext()) {
	    		Text currentText = textIterator.next();
	    		String cdata = currentText.toString();
	    		xml.append(cdata);
	    	}
	    	
	    	
	    	// conevrt the xml row to avro format using the given schema
	    	//String schemaString = context.getConfiguration().get("SCHEMASTRING");
	    	DatumBuilder datumBuilder = new DatumBuilder(Schema.parse(context.getConfiguration().get("SCHEMASTRING")));
	    	
	        Object mydatum = datumBuilder.createDatum(xml.toString());
	        AvroKey<GenericRecord> keyout = new AvroKey<GenericRecord>((GenericRecord) mydatum);
	        
	        //AvroKey<Object> test = new AvroKey<Object>(mydatum);
	    	context.write(keyout, NullWritable.get());
	    }
	  }

  // method to create the job and run it   
  @Override
  public int run(String[] args) throws Exception {
 
/***********	  
        if (args.length != 2) {
        	System.out
          .printf("Two parameters need to be supplied - <input dir> and <output dir>\n");
          return -1;
       }
**********/	  
	  

	  Configuration conf = new Configuration();
	  conf.set("mapred.child.java.opts", "-Xmx2048m -Xincgc");
  	  conf.set("SCHEMASTRING", schema.toString());
  	  
      Job job = new Job(conf);
      job.setJarByClass(SeqXmlToAvroConverterJobFlume.class);
      job.setJobName("Sequence file to Sequence file test job");
 
      FileInputFormat.addInputPath(job, new Path(args[0]));
      //FileInputFormat.addInputPath(job, new Path("/apps/DataLake/Development/test/test_akash/raw/"+ directoryFormat.format(new Date()) ) );
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      //FileOutputFormat.setOutputPath(job, new Path( "/apps/DataLake/Development/test/test_akash/decomposed/" + directoryFormat.format(new Date()) ) );
    
      //job.setInputFormatClass(SequenceFileInputFormat.class);
      job.setInputFormatClass(MyCustomInputFormat.class);
      //job.setOutputFormatClass(AvroKeyOutputFormat.class); 
      job.setOutputFormatClass(CustomAvroKeyOutputFormat.class);
      //  AvroJob.setOutputKeySchema(job, SCHEMA);			
      job.setMapperClass(SeqXmlToSeqAvroMapper.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      
      job.setReducerClass(SeqXmlToSeqAvroReducer.class);
      job.setNumReduceTasks(1);
      AvroJob.setOutputKeySchema(job, schema);
      
      //job.setOutputKeyClass(Text.class);
      //job.setOutputValueClass(Text.class);
 
      boolean success = job.waitForCompletion(true);
      return success ? 0 : 1;
   	}
  	
  	// not needed -- to be removed 
    public static void main(String[] args) throws Exception {
    	
    	int exitCode = ToolRunner.run(new Configuration(), new SeqXmlToAvroConverterJobFlume(), args);
    	System.exit(exitCode);
    }
}