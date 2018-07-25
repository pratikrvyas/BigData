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
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
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
 

@SuppressWarnings("deprecation")
public class SeqXmlToSeqAvro extends Configured implements Tool {
 
	public static final Schema schema = createSchema();
	private static Schema createSchema() {
	    try {
	    	String content = new Scanner(new File("test.avsc")).useDelimiter("\\Z").next();
	    	//Logger.getGlobal().info("FILE CONTENT IS --> "+ content);     
	        return Schema.parse(content);
	    } catch (final Exception exc) {
	        throw new Error(exc);
	    }  
	}

	private static final SimpleDateFormat directoryFormat = new SimpleDateFormat("yyyy-MM-dd");

	/************
	// Static schema to avro output 
	@SuppressWarnings("deprecation")
	public static final Schema schema = Schema.parse( "{\"type\" : \"record\",\"name\" : \"BooksForm\",\"fields\" : [ {\"name\" : \"book\",\"type\" : {\"type\" : \"array\",\"items\" : {\"type\" : \"record\",\"name\" : \"BookForm\", \"fields\" : [ { \"name\" : \"id\", \"type\" : [ \"null\", \"string\" ], \"source\" : \"attribute id\" }, {\"name\" : \"author\", \"type\" : \"string\", \"source\" : \"element author\"}, { \"name\" : \"title\", \"type\" : \"string\", \"source\" : \"element title\"}, {\"name\" : \"genre\",\"type\" : \"string\",\"source\" : \"element genre\" }, { \"name\" : \"price\", \"type\" : \"float\",\"source\" : \"element price\" }, {\"name\" : \"pub_date\",\"type\" : \"string\",\"source\" : \"element pub_date\"\n}, {\"name\" : \"review\",\"type\" : \"string\",\"source\" : \"element review\" } ]}},\"source\" : \"element book\"} ]}");
	*************/

	// Mapper class
    private static class SeqXmlToSeqAvroMapper extends Mapper<Text, Text, Object, NullWritable> {

    	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    		
	    	DatumBuilder datumBuilder = new DatumBuilder(Schema.parse(context.getConfiguration().get("SCHEMASTRING")));
	        Object mydatum = datumBuilder.createDatum(value.toString());
	        AvroKey<GenericRecord> keyout = new AvroKey<GenericRecord>((GenericRecord) mydatum);
    		context.write(keyout, NullWritable.get());
		}
    }
  
    // write identity reducer here ,for avro to combine all map outputs

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
      job.setJarByClass(SeqXmlToSeqAvro.class);
      job.setJobName("Sequence file to Sequence file test job");
 
      FileInputFormat.addInputPath(job, new Path(args[0]));
      //FileInputFormat.addInputPath(job, new Path("/apps/DataLake/Development/test/test_akash/raw/"+ directoryFormat.format(new Date()) ) );
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      //FileOutputFormat.setOutputPath(job, new Path( "/apps/DataLake/Development/test/test_akash/decomposed/" + directoryFormat.format(new Date()) ) );
    
      job.setInputFormatClass(SequenceFileInputFormat.class);
      job.setOutputFormatClass(AvroKeyOutputFormat.class);
 
      job.setMapperClass(SeqXmlToSeqAvroMapper.class);
      AvroJob.setOutputKeySchema(job, schema);
      
      job.setNumReduceTasks(0);
      
      boolean success = job.waitForCompletion(true);
      return success ? 0 : 1;
   	}
 
    public static void main(String[] args) throws Exception {
    	
    	int exitCode = ToolRunner.run(new Configuration(), new SeqXmlToSeqAvro(), args);
    	System.exit(exitCode);
    }
}