package edu.stevens.cs549.hadoop.pagerank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by tiankaixie on 12/8/16.
 */
public class NameReducer2 extends Reducer<DoubleWritable,Text,Text,Text> {
    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value:values) {
            context.write(value,new Text(key.toString()));
        }
    }
}
