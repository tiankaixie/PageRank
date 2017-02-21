package edu.stevens.cs549.hadoop.pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by tiankaixie on 12/8/16.
 */
public class NameReducer extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text name = new Text();
        Text weight = new Text();
        int i = 0;
        for (Text value: values) {
            if (value.toString().contains("name")){
                name.set(value.toString().substring(4));
                i++;
            }else {
                weight.set(value);
                i++;
            }
        }
        if (i==2){
            context.write(name,weight);
        }
        else {
            return;
        }
    }
}
