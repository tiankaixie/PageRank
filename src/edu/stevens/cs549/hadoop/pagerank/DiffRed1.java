package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffRed1 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double difference = 0.0;
		/* 
		 * TODO: The list of values should contain two ranks.  Compute and output their difference.
		 */
		for (Text value:values) {
			if (difference == 0.0){
				difference = Double.parseDouble(value.toString());
			}
			else {
				difference = Math.abs(difference - Double.parseDouble(value.toString()));
			}

		}
		context.write(key,new Text(Double.toString(difference)));
	}
}
