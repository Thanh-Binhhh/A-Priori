package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<Object, Text, Text, Text> {
    private final Text dateKey = new Text();
    private final Text customerValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");

        String customerId = parts[0].trim();
        String date = parts[1].trim();

        if (!customerId.equalsIgnoreCase("customer_id")) {
            dateKey.set(date);
            customerValue.set(customerId);
            context.write(dateKey, customerValue);
        }
    }
}