package com.kafka.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class HdfsWriterSequence {
	
	public void writeForHDFS(String message) {
        String filePath = "../kafka-hadoop/test.txt";
    	SequenceFile1 sequenceFile2 = new SequenceFile1();
    	IntWritable key = new IntWritable();
    	Text value = new Text();
    	key.set(1);
    	value.set(message);
    	   
        sequenceFile2.writeToSequenceFile(filePath, key, value);
    }
}
