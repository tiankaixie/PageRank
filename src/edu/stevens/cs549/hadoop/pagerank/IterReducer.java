package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double d = PageRankDriver.DECAY; // Decay factor
		/* 
		 * TODO: emit key:node+rank, value: adjacency list
		 * Use PageRank algorithm to compute rank from weights contributed by incoming edges.
		 * Remember that one of the values will be marked as the adjacency list for the node.
		 */
		double afterRank = 0.0;
		String adjacencyList = "";
		for (Text value: values) {
			String tempValue = value.toString();
			if (tempValue.contains("adj")){
				//this is the adjacency list
				adjacencyList = tempValue.substring(4);
			}else {
				afterRank += Double.parseDouble(tempValue);
			}
		}
		afterRank = (1-d) + d * afterRank;
		Text outputKey = new Text();
		outputKey.set(key.toString() + " " + afterRank);
		context.write(outputKey,new Text(adjacencyList));
	}
}
