package com.emirates.helix.macs;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WholeFileReader extends Configured implements Tool{
    
	private static final SimpleDateFormat directoryFormat = new SimpleDateFormat("yyyy-MM-dd");
	
    public static class wholeFileMapper extends Mapper<NullWritable, Text, Text, Text>{
        
        private Text fileName;
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
            InputSplit split = context.getInputSplit();
            Path path = ((FileSplit) split).getPath();
            fileName = new Text(path.toString());
        }
        
        @Override
        protected void map(NullWritable key, Text value,Context context) 
                throws IOException, InterruptedException{
            context.write(fileName, value);
        }
    }

    public static class wholeFileInputFormat extends FileInputFormat<NullWritable,Text>{
        
        @Override
        protected boolean isSplitable(JobContext context,Path file){
            return false;
        }
        
        @Override
        public RecordReader<NullWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) 
                throws IOException,InterruptedException{
            wholeFileRecordReader reader = new wholeFileRecordReader();
            reader.initialize(split,context);
            return reader; 
        }
    } 
    
    public static class wholeFileRecordReader extends RecordReader<NullWritable, Text> {
        private FileSplit fileSplit;
        private Configuration conf;
        private Text value = new Text();
        private boolean processed = false;
        
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) 
                throws IOException,InterruptedException{
            this.fileSplit = (FileSplit) split;
            this.conf = context.getConfiguration();
        }
        
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException{
            if (!processed){
                byte[] contents = new byte[(int) fileSplit.getLength()];
                Path file = fileSplit.getPath();
                FileSystem fs = file.getFileSystem(conf);
                FSDataInputStream in = null;
                try{
                    in = fs.open(file);
                    IOUtils.readFully(in, contents, 0, contents.length);
                    value.set(contents,0,contents.length);
                }finally{
                    IOUtils.closeStream(in);
                }
                processed = true;
                return true;
            }
            return false;
        }
        
        @Override
        public NullWritable getCurrentKey() throws IOException, InterruptedException{
            return NullWritable.get();
        }
        
        @Override
        public Text getCurrentValue() throws IOException, InterruptedException{
            return value;
        }
        
        @Override
        public float getProgress() throws IOException{
            return processed ? 1.0f : 0.0f;
        }
        
        @Override
        public void close() throws IOException{
            // do nothing
        }
    }
    
    public int run(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.setBoolean("mapred.output.compress", true);
		conf.set("mapred.output.compression.type", CompressionType.BLOCK.toString());
		conf.setClass("mapred.output.compression.codec", SnappyCodec.class, CompressionCodec.class);
		conf.set("mapreduce.reduce.input.limit", "100000024");
		 

        Job job = new Job(conf,"Whole File Input Format");
        job.setJarByClass(WholeFileReader.class);

        job.setMapperClass(wholeFileMapper.class);
        job.setInputFormatClass(wholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path("/apps/DataLake/Development/test/test_akash/raw/" + directoryFormat.format(new Date()) ));
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        return job.waitForCompletion(true) ? 0:1;
    }

    public static void main(String[] args) throws Exception{
        int exitcode = ToolRunner.run(new WholeFileReader(), args);
        System.exit(exitcode);
    }
}