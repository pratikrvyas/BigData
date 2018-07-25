package com.emirates.helix.macs.driver;

import org.apache.hadoop.util.ToolRunner;

import com.emirates.helix.macs.WholeFileReader;

public class ConvertXmlToAvro {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
        int seqexitcode = ToolRunner.run(new WholeFileReader(), args);
        if(seqexitcode == 0){
        	//int avroexitcode = ToolRunner.run(new TestSeqXmlToSeqAvroJob(), args);
        	int avroexitcode = ToolRunner.run(new SeqXmlToAvroConverterJob(), args);
        	System.exit(avroexitcode);
        }else{
        	System.exit(seqexitcode);
        }

	}

}
