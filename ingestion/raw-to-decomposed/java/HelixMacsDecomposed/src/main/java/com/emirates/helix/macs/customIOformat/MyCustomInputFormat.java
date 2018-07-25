package com.emirates.helix.macs.customIOformat;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import com.emirates.helix.macs.mrutils.InputSplitUtils;


public class MyCustomInputFormat extends SequenceFileInputFormat {

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
	 
		return InputSplitUtils.filterProcessedSplits( super.getSplits(job), job);
	
	}
	
}
