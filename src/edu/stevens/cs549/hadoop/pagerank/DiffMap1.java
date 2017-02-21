package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffMap1 extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		String[] sections = line.split("\t"); // Splits each line
		if (sections.length > 2) // checks for incorrect data format
		{
			throw new IOException("Incorrect data format of sections" + sections.length);
		}
		/**
		 *  TODO: read node-rank pair and emit: key:node, value:rank
		 */
		String [] nodeRantPair = sections[0].split(" ");
		if (nodeRantPair.length!=2) {
			throw new IOException("Incorrect data format of node rant pair " + nodeRantPair.length + " 1:"
					+ nodeRantPair[0] + " 2:" + nodeRantPair[1] + " 3:" + nodeRantPair[2]);
		}
		context.write(new Text(nodeRantPair[0]),new Text(nodeRantPair[1]));
	}

}
