package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/* 
		 * TODO: Output key: node+rank, value: adjacency list
		 */
		Text outputKey = new Text();
		for (Text value: values) {
			String tempValue1 = value.toString();
			String []tempValue2 = tempValue1.split(": ");
			outputKey.set(tempValue2[0] +" " + 1.00);
			context.write(outputKey,new Text(tempValue2[1]));
		}
	}
}
