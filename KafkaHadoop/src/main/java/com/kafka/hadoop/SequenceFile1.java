package com.kafka.hadoop;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenceFile1 {

	public static void writeToSequenceFile(String filePath, IntWritable key, Text value) {
		String hdfsUri = "hdfs://hadoop-master:9000"; 

		try {
			Configuration conf = new Configuration();
			conf.set("fs.default.name", hdfsUri);

			FileSystem fs = FileSystem.get(conf);
			Path outputPath = new Path(filePath);

			// Create SequenceFile.Writer
			SequenceFile.Writer.Option keyClass = SequenceFile.Writer.keyClass(IntWritable.class);
			SequenceFile.Writer.Option valueClass = SequenceFile.Writer.valueClass(Text.class);
			SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(outputPath), keyClass,
					valueClass);

			// Write data to SequenceFile
			Random random = new Random();
			int randomNumber = random.nextInt(10000) + 1;
			key.set(randomNumber);
            writer.append(key, value);
			writer.close();

			System.out.println("SequenceFile has been written to HDFS successfully.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void readFromSequenceFile(String filePath) {
		String hdfsUri = "hdfs://hadoop-master:9000"; 

		try {
			Configuration conf = new Configuration();
			conf.set("fs.default.name", hdfsUri);

			FileSystem fs = FileSystem.get(conf);
			Path inputPath = new Path(filePath);

			// Open SequenceFile.Reader
			SequenceFile.Reader.Option fileOption = SequenceFile.Reader.file(inputPath);
			SequenceFile.Reader reader = new SequenceFile.Reader(conf, fileOption);

			// Read key-value pairs from SequenceFile
			IntWritable key = new IntWritable();
			Text value = new Text();
			while (reader.next(key, value)) {
				System.out.println("Key: " + key.get() + ", Value: " + value.toString());
			}

			// Close the SequenceFile.Reader
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
