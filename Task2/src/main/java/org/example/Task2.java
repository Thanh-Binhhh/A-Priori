package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Task2 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length < 4) {
            System.out.println("Usage: Task2 <threshold> <input path> <output path for task 1> <output path for task 2>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.setInt("threshold", Integer.parseInt(args[0]));

        FileSystem fs = FileSystem.get(conf);
        Path filePath = new Path(args[1]);
        Path outputPath1 = new Path(args[2]);
        Path outputPath2 = new Path(args[3]);

        if (!fs.exists(filePath)) {
            System.out.println("File not found!");
            fs.close();
            System.exit(1);
        }

        // Pass 1
        Job job1 = Job.getInstance(conf, "APriori Algorithm - Pass 1");

        job1.setJarByClass(Task2.class);
        job1.setMapperClass(Pass1.MyMapper1.class);
        job1.setReducerClass(Pass1.MyReducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, filePath);

        if (fs.exists(outputPath1)) {
            fs.delete(outputPath1, true);
        }
        FileOutputFormat.setOutputPath(job1, outputPath1);

        if (!job1.waitForCompletion(true)) {
            fs.close();
            System.exit(1);
        }

        // Pass 2
        conf.set("outputPathPass1", args[2]);
        Job job2 = Job.getInstance(conf, "APriori Algorithm - Pass 2");

        job2.setJarByClass(Task2.class);
        job2.setMapperClass(Pass2.MyMapper2.class);
        job2.setReducerClass(Pass2.MyReducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, filePath);

        if (fs.exists(outputPath2)) {
            fs.delete(outputPath2, true);
        }
        FileOutputFormat.setOutputPath(job2, outputPath2);

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
        fs.close();
    }
}