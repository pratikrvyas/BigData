package com.emirates.helix.macs;


import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SequenceReducer extends Reducer<Text, Text, Text,NullWritable> {
	   
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
   
        for (Text val : values) {
        	
        	File xsdFile= new File("PAXXSD");
        	String xml=val.toString();
    

      	  SchemaBuilder schemaBuilder = new SchemaBuilder();
      	  Schema schema =null;
         
            if (xsdFile != null){
             schema = schemaBuilder.createSchema(xsdFile);
            }


                context.write(new Text(schema.toString(true)),null);
          
            DatumBuilder datumBuilder = new DatumBuilder(schema);
            Object datum = datumBuilder.createDatum(xml);

            /*
            try (OutputStream stream = new FileOutputStream(avroFile)) {
                DatumWriter<Object> datumWriter = new SpecificDatumWriter<>(schema);
                datumWriter.write(datum, EncoderFactory.get().directBinaryEncoder(stream, null));
            }
            */
        	
        }
        
    }
  }
