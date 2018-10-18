package repliespertweet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Created by ubuntu on 2/6/17.
 */
public class RepliesPerTweet {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: RepliesPerTweet <input path> <output path>");
            System.exit(-1);
        }

        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "Count Tweets");
        job.setJarByClass(RepliesPerTweet.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(RepliesPerTweetMapper.class);
        job.setReducerClass(RepliesPerTweetReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
