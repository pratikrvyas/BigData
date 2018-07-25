package com.emirates.helix.macs.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 

public class SeqXmlToSeqAvroIdentityMapper extends Mapper<Text, Text, Text, Text> {

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		context.write(key, value);
	}
}