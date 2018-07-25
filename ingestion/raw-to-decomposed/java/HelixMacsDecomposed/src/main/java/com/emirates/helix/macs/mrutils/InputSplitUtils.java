package com.emirates.helix.macs.mrutils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InputSplitUtils {

		public static List<InputSplit> filterProcessedSplits(List<InputSplit> inputSplits, JobContext job) throws IOException {
			 
			List<String> processedSplitIds = new ArrayList<String>();
			FileSystem hdfs = FileSystem.get(job.getConfiguration());
			 
			Path outputDir = FileOutputFormat.getOutputPath(job);
			Path splitsDir = new Path(outputDir, ".meta" + "/splits");
			 
			FileStatus splitFiles[] = hdfs.listStatus(splitsDir);
			 
			if (splitFiles == null) {
				return inputSplits;
			}
			 
			for (FileStatus s : splitFiles) {
				processedSplitIds.add(s.getPath().getName());
			}
			 
			List<InputSplit> unprocessedSplits = new ArrayList<InputSplit>();
			 
			for (InputSplit split : inputSplits) {
				//String splitId = MD5Hash.generate(split.toString());
				String splitId = MD5Hash.digest(split.toString()).toString();
				if (!processedSplitIds.contains(splitId)) {
					unprocessedSplits.add(split);
				}
			}
			return unprocessedSplits;
		}
			 
		public static String getInputSplitId(InputSplit split) {
			 
			//return MD5Hash.generate(split.toString());
			return MD5Hash.digest(split.toString()).toString(); 
		}
			 
		public static void saveInputSplitId(Context context, String splitId) throws IOException, InterruptedException {
			 
			Path workingDir = FileOutputFormat.getWorkOutputPath(context);
			FileSystem hdfs = FileSystem.get(context.getConfiguration());
			Path path = new Path(workingDir, ".meta/splits/" + splitId);
			hdfs.create(path).close();
			 
		}
				
	
}
