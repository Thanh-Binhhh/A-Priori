package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class MyReducer extends Reducer<Text, Text, Text, Text> {
    Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<String> customers = new HashSet<>();

        for (Text val : values) {
            customers.add(val.toString());
        }

        result.set(customers.toString());
        context.write(key, result);
    }
}