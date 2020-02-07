package com.developer.abhinavraj.requestsummary;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RequestSummaryReducer extends Reducer<Text, Summary, Text, Summary> {

    @Override
    public void reduce(Text key, Iterable<Summary> values,
                       Context context)
            throws IOException, InterruptedException {

        int counter = 0;
        int downloadSize = 0;
        Summary temp;
        for (Summary value : values) {
            counter += value.getRequests();
            downloadSize += value.getDownloadSize();
        }
        temp = new Summary(counter,downloadSize);
        context.write(key, temp);
    }
}
