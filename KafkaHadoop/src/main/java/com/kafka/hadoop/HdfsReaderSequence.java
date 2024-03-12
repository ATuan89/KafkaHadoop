package com.kafka.hadoop;

public class HdfsReaderSequence {
	
	public static void main(String[] args) {
    	SequenceFile1 sequenceFile = new SequenceFile1();
        String filePath = "../kafka-hadoop/test.txt"; // Specify the path to the file in HDFS
        sequenceFile.readFromSequenceFile(filePath);
    }
}
