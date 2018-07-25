package com.emirates.helix.macs.customIOformat;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

public class CustomAvroKeyOutputFormat extends AvroKeyOutputFormat {
	
/*************	
	@Override
	 protected OutputStream getAvroFileOutputStream(TaskAttemptContext context) throws IOException {
		     Path path = new Path(((FileOutputCommitter)getOutputCommitter(context)).getWorkPath(),
		       getUniqueFile(context,context.getConfiguration().get("avro.mo.config.namedOutput","part"),org.apache.avro.mapred.AvroOutputFormat.EXT));
		     //return path.getFileSystem(context.getConfiguration()).create(path);
		     return path.getFileSystem(context.getConfiguration()).append(path);
		   }
*************/
	@Override
	public void checkOutputSpecs(JobContext job) throws FileAlreadyExistsException, IOException{
			    
			    Path outDir = getOutputPath(job);
			    if (outDir == null) {
			      throw new InvalidJobConfException("Output directory not set.");
			    }
			    /*******
			    // Ensure that the output directory is set and not already there
			    if (outDir.getFileSystem(job.getConfiguration()).exists(outDir)) {
			      throw new FileAlreadyExistsException("Output directory " + outDir + 
			                                           " already exists");
			    }
			    ***********/
	}
	
	
}
