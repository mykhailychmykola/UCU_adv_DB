package com.DB.kolya;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IntWritable;
//import com.lits.kundera.test.Util;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;

public class MyReducer extends Reducer<Text, IntWritable,
        Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        for(IntWritable val : values) {
            i += val.get();
        }
        byte[] patientId = Arrays.copyOfRange(key.copyBytes(), 0, 16);
        context.write(new Text(SuperUtil.toString(patientId, patientId.length)), new IntWritable(i));
    }
}
