import java.io.IOException;
import java.util.*;
import java.util.Map;
import java.lang.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.ArrayList;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.WritableComparable;

public class WordCount {

	// Does the mapping
	public static class TokenizerMapper
	  extends Mapper<Object,Text,Text,IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// Split String based on tabs
			String res[] = value.toString().split("\t+");
		
			// Ignore headers
			if (res[2].equals("conference_location"))
				return;

			// Stores the conference_location as a Text
			word.set(res[2]);

			// Output the key value pair
			context.write(word, one);
		}
	}
	
	// Reducing Class
	public static class IntSumReducer
	  extends Reducer<Text,IntWritable,Text,IntWritable> {

		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context
		) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	// Main Function
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}		
