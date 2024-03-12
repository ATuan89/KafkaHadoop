package com.kafka.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

public class ReadHdfs {
    public static void main(String[] args) {
    	SequenceFile1 sequenceFile = new SequenceFile1();
    	
        String hdfsUri = "hdfs://hadoop-master:9000"; // HDFS URI
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://hadoop-master:9000");        
        
        try {
            String filePath = "/home/tuan/Documents/new1.txt"; // Specify the path to the file in HDFS
            FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf);
            Path hdfsPath = new Path(filePath);
            sequenceFile.readSequenceFile(filePath);

            
            // Open an FSDataInputStream to read data from the file
            FSDataInputStream inputStream = fs.open(hdfsPath);

            // Wrap the FSDataInputStream in a BufferedReader for efficient reading
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            // Read data line by line from the file
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line); // Print each line of data
            }

            // Close the reader and input stream
            reader.close();
            inputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

