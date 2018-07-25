package com.emirates.helix.macs.reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.emirates.helix.macs.DatumBuilder;

public class SeqXmlToSeqAvroReducer extends Reducer<Text, Text, Object, NullWritable> {

	  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException {

		    StringBuffer xml =  new StringBuffer();
	 	    Iterator<Text> textIterator = values.iterator();
	    	while (textIterator.hasNext()) {
	    		Text currentText = textIterator.next();
	    		String cdata = currentText.toString();
	    		xml.append(cdata);
	    	}
	    	
	    	
	    	// convert the xml row to avro format using the given schema
	    	//String schemaString = context.getConfiguration().get("SCHEMASTRING");
	    	DatumBuilder datumBuilder = new DatumBuilder(Schema.parse(context.getConfiguration().get("SCHEMASTRING")));
	    	
	        Object mydatum = datumBuilder.createDatum(xml.toString());
	        AvroKey<GenericRecord> keyout = new AvroKey<GenericRecord>((GenericRecord) mydatum);
	        
	        //AvroKey<Object> test = new AvroKey<Object>(mydatum);
	    	context.write(keyout, NullWritable.get());
	   }
}
