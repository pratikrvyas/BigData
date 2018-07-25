package com.emirates.helix.macs.driver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.URI;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapred.FileSplit;

import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.emirates.helix.macs.DatumBuilder;
import com.emirates.helix.macs.mrutils.PreMRJobUtils;
 

@SuppressWarnings("deprecation")
public class SeqXmlToAvroUuid extends Configured implements Tool {

	//date format to append to output folder name 
	private static final SimpleDateFormat directoryFormat = new SimpleDateFormat("yyyy-MM-dd");

	// Mapper class
    private static class SeqXmlToAvroMapper extends Mapper<LongWritable, BytesWritable, Text, Text> {
    	@Override
    	public void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
    		
    		String fileName = null;
    		
    		// Mapper to fetch xml, convert it to avro 
	    	DatumBuilder datumBuilder = new DatumBuilder(Schema.parse(context.getConfiguration().get("SCHEMASTRING")));
	    	byte[] b = new byte[value.getLength()];
	    	InputStream out = new ByteArrayInputStream(value.getBytes());
	    	//out.write(value.getBytes());
	        out.read(b);
	        //System.out.println("WRITING DATA TO CONTEXT TO REDUCER !!!");
	        //Logger.getGlobal().info("WRITING DATA TO CONTEXT TO REDUCER !!!");
	        context.write(new Text(String.valueOf(UUID.randomUUID())), new Text(new String(b)));
    		
    	}
    	
     }

    // Reducer class
    private static class IdentityAvroReducer extends Reducer<Text, Text, Object, NullWritable> {
    	
    	@Override
    	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
    		
    		//System.out.println("SINGLE REDUCER STARTED !!!");
    		//Logger.getGlobal().info("SINGLE REDUCER STARTED !!!");
    		StringBuffer xml = new StringBuffer();
	 	    Iterator<Text> textIterator = values.iterator();
	    	while (textIterator.hasNext()) {
	    		Text currentText = textIterator.next();
	    		
	    		String cdata = currentText.toString();
	    		xml.append(cdata);
	    	}
	    	
	    	DatumBuilder datumBuilder = new DatumBuilder(Schema.parse(context.getConfiguration().get("SCHEMASTRING")));
	    	Object mydatum = datumBuilder.createDatum(new String(xml));
	    	
	        //System.out.println("DATUM VALUE IS --" +mydatum.toString());
	        AvroKey<GenericRecord> keyout = new AvroKey<GenericRecord>((GenericRecord) mydatum);
	    	
	        //System.out.println("SINGLE REDUCER ENDED ... WRITING DATA TO HDFS !!!");
	        //Logger.getGlobal().info("SINGLE REDUCER ENDED ... WRITING DATA TO HDFS !!!");
    		context.write( keyout, NullWritable.get());
	    }
    	
    	
	  }


    
  @Override
  public int run(String[] args) throws Exception {
 
  
        if (args.length != 4) {
        	System.out.printf("Four parameters need to be supplied - <input dir> <output dir> <schema file path> <hadoop namenode path>\n");
          return -1;
       }

        // load schema of avro output
	  	Schema schema = null;
	    try {
	    	//System.out.println("READING SCHEMA !!! ");
	    	//Logger.getGlobal().info("READING SCHEMA !!! ");
	    	//Path pt=new Path("hdfs://dxbhqhdphn01.hq.emirates.com:8020/apps/DataLake/Development/test/test_akash/test.avsc");//Location of file in HDFS
	    	//Path pt=new Path("hdfs://dxbhqhdphn01.hq.emirates.com:8020" + args[2]);//Location of file in HDFS
	    	Path pt=new Path(args[3] + args[2]);//Location of file in HDFS
	    	FileSystem fs = FileSystem.get(new Configuration());
	    	BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
	    	StringBuffer doc = new StringBuffer();
	    	String line;
	    	line=br.readLine();
	    	while (line != null){
	    		//System.out.println(line);
	    		doc.append(line);
	    		line=br.readLine();
            
	    	}
	    	//Logger.getGlobal().info("FILE CONTENT IS --> "+ doc.toString());
	    	//System.out.println("SCHEMA READ DONE !!! ");	
	    	//Logger.getGlobal().info("SCHEMA READ DONE !!! " );
	    	schema = Schema.parse(doc.toString()); 
	    }catch (final Exception exc) {
	        throw new Error(exc);
	    	//return null;
	    }

	  String outputPath = args[1];  
	  String inputPath = args[0];
	  int corruptedFiles = 0;
	  
	  PreMRJobUtils preMRJobUtils = new PreMRJobUtils();
	  corruptedFiles = preMRJobUtils.removeCorruptedFiles(inputPath);
	    
	  Configuration conf = new Configuration();
	  conf.set("mapred.child.java.opts", "-Xmx1024m -Xincgc");
      
	  conf.setBoolean("mapreduce.map.output.compress", true);
	  //conf.set("mapred.output.compression.type", CompressionType.BLOCK.toString());
      conf.setClass("mapred.map.output.compression.codec", SnappyCodec.class, CompressionCodec.class);
      conf.set("mapred.tasktracker.map.tasks.maximum", "6");
      conf.set("mapred.tasktracker.reduce.tasks.maximum","1");
      conf.set("io.sort.mb", "512");
      conf.set("io.sort.factor","51");
      
      // enable appending in hdfs 
      conf.set("dfs.support.append", "true");
      
	  conf.set("SCHEMASTRING", schema.toString());
  	  
      Job job = new Job(conf);
      job.setJarByClass(SeqXmlToAvroUuid.class);
      job.setJobName("Job to Convert Sequence file to Avro file");
 
      FileInputFormat.addInputPath(job, new Path(args[0]));
      //FileInputFormat.addInputPath(job, new Path("/apps/DataLake/Development/test/test_akash/raw/"+ directoryFormat.format(new Date()) ) );
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      //FileOutputFormat.setOutputPath(job, new Path( "/apps/DataLake/Development/test/test_akash/decomposed/" + directoryFormat.format(new Date()) ) );
    
      job.setInputFormatClass(SequenceFileInputFormat.class);
      job.setOutputFormatClass(AvroKeyOutputFormat.class);
 
      job.setMapperClass(SeqXmlToAvroMapper.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      //AvroJob.setMapOutputValueSchema(job, schema);
      AvroJob.setOutputKeySchema(job, schema);
      
      job.setReducerClass(IdentityAvroReducer.class);
      job.setNumReduceTasks(1);
      boolean success = job.waitForCompletion(true);
      
      long inputRecords = 0;
      long outputRecords = 0;
      // accessing counters 
      for (CounterGroup group : job.getCounters()) {   
    	  //System.out.println("- Counter Group: " + group.getDisplayName() + " (" + group.getName() + ")");
    	  if(group.getName().equals("org.apache.hadoop.mapreduce.TaskCounter")){
    		  //System.out.println(" number of counters in this group: " + group.size());
    		  for ( Counter counter : group) {
    	        //System.out.println(" - " + counter.getDisplayName() + ": " + counter.getName() + ": " +counter.getValue());
    	        if(counter.getName().equals("MAP_INPUT_RECORDS")){
    	        	inputRecords = counter.getValue();	
    	        }
    	        if(counter.getName().equals("REDUCE_OUTPUT_RECORDS")){
    	        	outputRecords = counter.getValue();	
    	        }
    		  }
    	  }
      }//for ends here -- org.apache.hadoop.mapreduce.TaskCounter
      
      System.out.println(" INPUT RECORDS -- " + inputRecords);
      System.out.println(" OUTPUT RECORDS -- " + outputRecords);
      System.out.println("writing to file !!!");
      // outputPath -- last / and 
      String uri = "hdfs://dxbhqhdphn01.hq.emirates.com:8020/apps/helix/reports/incrementalstats/";
      System.out.println("basic url is -- "+ uri);
      // replace test with pax
      if ( outputPath.contains("pax")){
    	  uri = uri+"pax.txt";
    	  
      }
      else if( outputPath.contains("flt")){
    	  uri = uri+"flt.txt";
    	  
      }else {
    	  return success ? 0 : 1;
      }
      
      System.out.println("PATH IS -- " + uri);
      String[] splitOutputPath = outputPath.split("/");
      String content = splitOutputPath[splitOutputPath.length-1] +" \t" +inputRecords +" \t"+ outputRecords +" \t"+ corruptedFiles;
      System.out.println("Conetnt to append is -- "+ content);
      final Path path = new Path(uri);
      // get a HDFS filesystem instance
   	  FileSystem fs = FileSystem.get(URI.create(uri), conf);
   	  if(!fs.exists(path)){
		   System.out.println("File not exists !!! creating file !!!!!");
		   fs.createNewFile(path);
	   }
   	  FSDataOutputStream fsout = fs.append(path);
      // wrap the outputstream with a writer
      PrintWriter writer = new PrintWriter(fsout);
   	  writer.append(content + "\n");
   	  writer.close();
   	  
      return success ? 0 : 1;
   	}
 
    public static void main(String[] args) throws Exception {
    	//System.out.println("STARTING JOB -- RUN METHOD");
    	//Logger.getGlobal().info("STARTING JOB -- RUN METHOD");
    	int exitCode = ToolRunner.run(new Configuration(), new SeqXmlToAvroUuid(), args);
    	//System.out.println("JOB COMPLETED :) !!!");
    	//Logger.getGlobal().info("JOB COMPLETED :) !!!");
    	System.exit(exitCode);
    }
}