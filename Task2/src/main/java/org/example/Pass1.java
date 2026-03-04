package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class Pass1 {

    public static class MyMapper1 extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text customer = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(handle(value.toString()), ", ");

            while (tokenizer.hasMoreTokens()) {
                customer.set(tokenizer.nextToken());
                context.write(customer, one);
            }
        }

        public static String handle(String str) {
            return str.replaceAll("[\\[\\]]", "")
                    .split("\t")[1];
        }
    }

    public static class MyReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private int threshold;

        @Override
        protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            threshold = context.getConfiguration().getInt("threshold", 0);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }

            if (sum >= threshold) {
                result.set(sum);
                context.write(key, result);
            }
        }
    }
}
