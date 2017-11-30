package assignment1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;

/**
 * Created by zehafta on 11/28/17.
 */
public class MyReducer extends Reducer<Text, Text, Text, IntWritable> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String video_id = null;
        int ratings = Integer.MIN_VALUE;

        for (Text value : values) {

            String data[] = value.toString().split(":");

            int ratingfromMapper = Integer.parseInt(data[1]);
            System.out.println("FROM: "+data[0]);


            if (video_id != null) {
                if (ratings < ratingfromMapper) {
                    ratings = ratingfromMapper;
                    video_id = data[0];
                }
            }
            else {
                video_id = data[0];
                ratings = ratingfromMapper;
            }
        }
        context.write(new Text(video_id),new IntWritable(ratings));
    }

}
