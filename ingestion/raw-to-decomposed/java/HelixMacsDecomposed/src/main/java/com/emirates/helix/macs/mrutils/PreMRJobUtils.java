package com.emirates.helix.macs.mrutils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PreMRJobUtils {

	public int removeCorruptedFiles(String path) throws Exception{
		
		String inPath = "hdfs://dxbhqhdphn01.hq.emirates.com:8020"+path;
		//String[] splitInPath = inPath.split("/");
		String str = new Path(path).getParent().toString();
		String inPathDate = path.split("/")[path.split("/").length - 2];
		//String outPath = "hdfs://dxbhqhdphn01.hq.emirates.com:8020/"+str+"/corrupted/";
		String outPath = "hdfs://dxbhqhdphn01.hq.emirates.com:8020/data/helix/rejected/raw/macs/";
		
		if ( inPath.contains("pax")){
	    	  outPath = outPath+"pax/"+inPathDate +"/" ;
	    	  System.out.println("OUTPATH --" + outPath);
	    	  
	      }
	      else if( inPath.contains("flt")){
	    	  outPath = outPath+"flt/"+inPathDate +"/" ;
	    	  System.out.println("OUTPATH --" + outPath);
	      }else {
	    	  return -1;
	      }
		
		int totalFiles = 0;
		int corruptedFiles = 0;
        FileSystem fs = FileSystem.get(new Configuration());
        FileStatus[] status = fs.listStatus(new Path(inPath));
        for (int i=0;i<status.length;i++){
        		totalFiles++;
        		try{
        			System.out.println("FILE NAME IS -- " + status[i].getPath().getName() );
        			System.out.println("PATH NAME IS -- " + status[i].getPath());
        			System.out.println("PARENT NAME IS -- " + status[i].getPath().getParent() );
        			fs.open(status[i].getPath());
        			//fs.close();
        			
        			
        		}catch(IOException e){
        			corruptedFiles++;
        			System.out.println("CATCH !!!");
        			System.out.println("OUTPATH --" + outPath);
        			if(!fs.exists(new Path(outPath))){
        				//fs.create(new Path(outPath));
        				fs.mkdirs(new Path(outPath));
        				
        			}
        			System.out.println("FROM --"+status[i].getPath());
        			System.out.println("TO --"+outPath);
        			System.out.println("IS OUT PATH A DIRECTORY -- "+fs.isDirectory(new Path(outPath)));
        			fs.rename(status[i].getPath(), new Path(outPath));
        			System.out.println("MOVE !!!");
        		}
        }
        
        System.out.println("TOTAL FILES -- "+totalFiles);
        System.out.println("CORRUPTED FILES -- "+corruptedFiles);

		
		return corruptedFiles;
		
	}
	
}
