import org.json.JSONObject;

import java.io.IOException;
import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class RedditAverage extends Configured implements Tool {

	public static class MyMapper
	extends Mapper<LongWritable, Text, Text, LongPairWritable>{

			private Text word = new Text();
			private LongPairWritable pair = new LongPairWritable();
			private final static Long one = new Long(1);

			@Override
			public void map(LongWritable key, Text value, Context context
							) throws IOException, InterruptedException {
					JSONObject record = new JSONObject(value.toString());

					String subreddit = (String) record.get("subreddit");
					int score = (Integer) record.get("score");

					word.set(subreddit);
					pair.set(one, score);
					context.write(word, pair);
					}
		}
	

	public static class MyReducer
	extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
			private DoubleWritable result = new DoubleWritable();

			@Override
			public void reduce(Text key, Iterable<LongPairWritable> pairs,
							Context context
							) throws IOException, InterruptedException {
					double sum = 0, count = 0;
					double avg = 0.0;
					for (LongPairWritable pair : pairs) {
							sum += (double) pair.get_1();
							count += (double) pair.get_0();
					}
					avg = (double)(sum / count);
					result.set(avg);
					context.write(key, result);
			}
	}

	public static class MyCombiner
        extends Reducer<Text, LongPairWritable, Text, LongPairWritable>{

                private LongPairWritable pair = new LongPairWritable();

                @Override
                public void reduce(Text key, Iterable<LongPairWritable> pairs,
                                Context context
                                ) throws IOException, InterruptedException {
                        long sum = 0 , count= 0;
                        for (LongPairWritable pair : pairs) {
                                sum += pair.get_1();
                                count += pair.get_0();
                        }
                        pair.set(count, sum);
                        context.write(key, pair);
                }
    }

public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
	System.exit(res);
}

@Override
public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "reddit average");
		job.setJarByClass(RedditAverage.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(MyReducer.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(LongPairWritable.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(DoubleWritable.class);

				TextInputFormat.addInputPath(job, new Path(args[0]));
				TextOutputFormat.setOutputPath(job, new Path(args[1]));

				return job.waitForCompletion(true) ? 0 : 1;
		}
}
