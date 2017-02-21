package edu.stevens.cs549.hadoop.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by tiankaixie on 12/8/16.
 */
public class NameMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException, IllegalArgumentException {
        String line = value.toString(); // Converts Line to a String
        if (line.contains(":")){
            String [] nodes = line.split("\n");
            for (String node:nodes) {
                String [] sections = node.split(": ");
                if (sections.length >2) throw new IOException("Invalid input : "+ sections.length);
                else if (sections.length==1) return;

                context.write(new Text(sections[0]),new Text("name" + sections[1]));
            }
        }else {
            String [] sections = line.split("\t");
            if (sections.length > 2) throw new IOException("Invalid input : " + sections.length);
            else if (sections.length == 1) return;
            context.write(new Text(sections[0]), new Text(sections[1]));
        }

    }
}
