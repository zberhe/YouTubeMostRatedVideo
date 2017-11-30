package assignment1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by zehafta on 11/28/17.
 */
public class MyMapper extends Mapper<LongWritable,Text,Text,Text> {
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

String data[] = value.toString().split("\t");
    if(data.length<7) return;
String numberOfRates = data[7];
context.write(new Text("KEY"),new Text(data[0]+":"+numberOfRates));
}
}
