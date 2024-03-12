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
    	
        String hdfsUri = "hdfs://hadoop-master:9000"; // HDFS URI
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://hadoop-master:9000");        
        try {
            String filePath = "../kafka-hadoop/test.txt"; // Specify the path to the file in HDFS
            FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf);
            Path hdfsPath = new Path(filePath);

            // Open an FSDataInputStream to read data from the file
            FSDataInputStream inputStream = fs.open(hdfsPath);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line); // Print each line of data
            }

            reader.close();
            inputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

