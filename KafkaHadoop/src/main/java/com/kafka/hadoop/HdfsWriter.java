package com.kafka.hadoop;

import java.io.BufferedOutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsWriter {
    
    public void writeForHDFS(String message) {
    	SequenceFile1 sequenceFile = new SequenceFile1();
    	
    	String hdfsUri = "hdfs://hadoop-master:9000"; // HDFS URI
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://hadoop-master:9000");        

        try {
            String filePath = "/home/tuan/Documents/new1.txt";
            FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf);
            sequenceFile.writeSequenceFile("1", message, filePath);
        	
            // Path object representing the file in HDFS
            Path hdfsPath = new Path(filePath);
            
            // Create an OutputStream to write data to the file
//            FSDataOutputStream os = fs.append(hdfsPath); 
            FSDataOutputStream os = fs.create(hdfsPath); 


            // BufferedOutputStream for better performance
            BufferedOutputStream bos = new BufferedOutputStream(os);

            // Sample data to write to the file
            String data = message;

            // Write data to the file
            bos.write(data.getBytes());

            // Close the streams
            bos.close();
            os.close();
            
            System.out.println("Data has been written to HDFS successfully.");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
    }
}

