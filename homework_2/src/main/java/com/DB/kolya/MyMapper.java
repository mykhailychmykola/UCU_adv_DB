package com.DB.kolya;

import com.google.common.primitives.Bytes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class MyMapper extends TableMapper<Text, IntWritable>{
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        IntWritable myInt = new IntWritable(1);
        byte[] record_patient_key = key.get();

        byte[] patient_key = Arrays.copyOfRange(record_patient_key, 16, 32);
        byte[] recordType = value.getValue("medical_records".getBytes(), "type".getBytes());
        byte[] patientKey_recordType = Bytes.concat(patient_key, recordType);

        context.write(new Text(patientKey_recordType), myInt);
    }
}
