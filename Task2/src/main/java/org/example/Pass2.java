package org.example;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

public class Pass2 {

    public static class MyMapper2 extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text pair = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] customers = handle(value.toString()).split(", ");

            for (int i = 0; i < customers.length - 1; i++) {
                for (int j = i + 1; j < customers.length; j++) {
                    pair.set(customers[i]+ ", " + customers[j]);
                    context.write(pair, one);
                }
            }
        }

        public static String handle(String str) {
            return str.replaceAll("[\\[\\]]", "")
                    .split("\t")[1];
        }
    }

    public static class MyReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
        private BloomFilter<String> customers;
        private Text pairKey = new Text();
        private IntWritable countValue = new IntWritable();
        private int threshold;

        @Override
        protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            threshold = context.getConfiguration().getInt("threshold", 0);
            Path inputPath = new Path(context.getConfiguration().get("outputPathPass1") + "/part-r-00000");

            // Ước tính 10 triệu khách hàng với tỷ lệ lỗi ~ 0.01%
            customers = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), 10_000_000, 0.0001);

            if (fs.exists(inputPath)) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inputPath)))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        customers.put(line.split("\t")[0]);
                    }
                }
            } else {
                System.err.println("File not found: " + inputPath.toString());
            }
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String[] pairCustomers = (key.toString()
                                        .split("\t")[0])
                                        .split(", ");

            int sum = 0;

            if (customers.mightContain(pairCustomers[0]) && customers.mightContain(pairCustomers[1])) {
                for (IntWritable value : values) {
                    sum += value.get();
                }

                if (sum >= threshold) {
                    countValue.set(sum);
                    pairKey.set("[" + key + "]");
                    context.write(pairKey, countValue);
                }
            }
        }
    }
}