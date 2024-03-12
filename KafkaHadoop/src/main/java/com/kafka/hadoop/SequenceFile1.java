package com.kafka.hadoop;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenceFile1 {

	public void writeSequenceFile(String key, String value, String filePath) throws Exception {
	    Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(URI.create(filePath), conf);
	    Path path = new Path(filePath);

	    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, Text.class, Text.class);
	    try {
	        writer.append(new Text(key), new Text(value));
	    } finally {
	        IOUtils.closeStream(writer);
	    }
	}
	
	public void readSequenceFile(String filePath) throws Exception {
	    Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(URI.create(filePath), conf);
	    Path path = new Path(filePath);

	    SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
	    try {
	        Text key = new Text();
	        Text value = new Text();
	        while (reader.next(key, value)) {
	            System.out.printf("Key: %s, Value: %s\n", key, value);
	        }
	    } finally {
	        IOUtils.closeStream(reader);
	    }
	}

}


