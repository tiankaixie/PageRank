package edu.stevens.cs549.hadoop.pagerank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by tiankaixie on 12/8/16.
 */
public class NameMapper2 extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException, IllegalArgumentException {
        String line = value.toString(); // Converts Line to a String
        String []sections = line.split("\t");
        if (sections.length!=2) throw new IOException("Invalid Input");
        context.write(new DoubleWritable(Double.parseDouble(sections[1])),new Text(sections[0]));
    }
}
