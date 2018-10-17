package totalkeywordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;



/**
 * Created by ubuntu on 2/6/17.
 */
public class CountTotalKeywordsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String tweet = value.toString();
        Status status = null;
        try {
            status = TwitterObjectFactory.createStatus(tweet);
        } catch (TwitterException e) {
            e.printStackTrace();
        }
        if(status == null) { return; }
        String text = status.getText();
        if(text == null) { return; }
        String[] words = text.toLowerCase().replaceAll("[;'\".,?/:]", "").replace(",","").split(" ");

        for(String w : words) {
            if(w.equals("trump")) {
                context.write(new Text("trump"), new IntWritable(1));
            }
            if(w.equals("zika")) {
                context.write(new Text("zika"), new IntWritable(1));
            }
            if(w.equals("flu")) {
                context.write(new Text("flu"), new IntWritable(1));
            }
            if(w.equals("diarrhea")) {
                context.write(new Text("diarrhea"), new IntWritable(1));
            }
            if(w.equals("ebola")) {
                context.write(new Text("ebola"), new IntWritable(1));
            }
            if(w.equals("headache")) {
                context.write(new Text("headache"), new IntWritable(1));
            }
            if(w.equals("measles")) {
                context.write(new Text("measles"), new IntWritable(1));
            }
        }
    }
}
