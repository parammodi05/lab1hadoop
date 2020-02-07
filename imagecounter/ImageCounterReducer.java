package com.developer.abhinavraj.imagecounter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ImageCounterReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context)
            throws IOException, InterruptedException {

        int counter = 0;
        for (IntWritable value : values) {
            counter += value.get();
        }
        context.write(key, new IntWritable(counter));
    }
}
