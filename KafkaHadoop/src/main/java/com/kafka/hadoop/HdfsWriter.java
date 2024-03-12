package com.kafka.hadoop;

import java.io.BufferedOutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class HdfsWriter {
    
    public void writeForHDFS(String message) {
    	IntWritable key = new IntWritable();
    	Text value = new Text();
    	key.set(1);
    	value.set(message);
    	
    	String hdfsUri = "hdfs://hadoop-master:9000"; // HDFS URI
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://hadoop-master:9000");        
        try {
            String filePath = "../kafka-hadoop/test.txt";
            FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf);

            // Path object representing the file in HDFS
            Path hdfsPath = new Path(filePath);
            
//            FSDataOutputStream os = fs.append(hdfsPath); 
            FSDataOutputStream os = fs.create(hdfsPath); 


            // BufferedOutputStream for better performance
            BufferedOutputStream bos = new BufferedOutputStream(os);
            String data = message;

            // Write data to the file
//            bos.write(data.getBytes());

            bos.close();
            os.close();
            
            System.out.println("Data has been written to HDFS successfully.");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
    }
}

