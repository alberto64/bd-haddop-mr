package repliespertweet;

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
public class RepliesPerTweetMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String tweet = value.toString();
        int index = tweet.indexOf("\"reply_count\":");
        int reply_Count = Integer.getInteger(String.valueOf(tweet.charAt(index + 15)));
        Status status = null;
        try {
            status = TwitterObjectFactory.createStatus(tweet);
        } catch (TwitterException e) {
            e.printStackTrace();
        }
        if(status == null) { return; }
        context.write(new Text(String.valueOf(status.getId())), new IntWritable(reply_Count));
    }
}
