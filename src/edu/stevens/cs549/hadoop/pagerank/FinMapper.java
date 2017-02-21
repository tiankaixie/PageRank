package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class FinMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		String[] sections = line.split("\t");
		/*
		 * TODO output key:-rank, value: node
		 * See IterMapper for hints on parsing the output of IterReducer.
		 */
		if (sections.length >2) throw new IOException("Invalid input : "+ sections.length);
		else if (sections.length==1) return;
		String [] nodeRankPair = sections[0].split(" ");
		if (nodeRankPair.length!=2) throw new IOException("Invalid Input");
		context.write(new DoubleWritable(Double.parseDouble(nodeRankPair[1])),new Text(nodeRankPair[0]));
	}

}
