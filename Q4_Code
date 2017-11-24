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

	private static class CompositeGroupKey implements WritableComparable<CompositeGroupKey> {
		String conf_city;
		String conf_year;
		public CompositeGroupKey() {}
		public CompositeGroupKey(String conf_city, String conf_year) {
			this.conf_city = conf_city;
			this.conf_year = conf_year;
		}
		public void write(DataOutput out) throws IOException {
			WritableUtils.writeString(out, conf_city);
			WritableUtils.writeString(out, conf_year);
		}
		public void readFields(DataInput in) throws IOException {
			this.conf_city = WritableUtils.readString(in);
			this.conf_year = WritableUtils.readString(in);
		}
		public int compareTo(CompositeGroupKey pop) {
			if (pop == null)
				return 0;
			int count = conf_city.compareTo(pop.conf_city);
			return count == 0 ? conf_year.compareTo(pop.conf_year) : count;
		}
		@Override
		public String toString() {
			return conf_city.toString() + "\t" + conf_year.toString();
		}	
	}	

	// Should ouput City1 Year1 Number_of_Conferences1
	// 		      Year2 Number_of_Conferences2
	// 		      ...
	// 		City2 ...

	//	Year is inside the Conf_Acronym

	public static class TokenizerMapper
		extends Mapper<Object, Text, CompositeGroupKey, IntWritable>{
		
    		private final static IntWritable one = new IntWritable(1);
    		//private Text city = new Text();
   		//private Text year = new Text();
    		//private Text no_Conf = new Text();	// Number of Conferences 
 
    		public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
			// Split on tabs, ie. res1[0] = acronym, res1[1] = name, res1[2] = city
			String[] res1 = value.toString().split("\t");
			// Split acronym on commas (may not be needed)	Data would have to be cleaned to have commas before year
			String[] acron = res1[0].split(",");
			CompositeGroupKey word = new CompositeGroupKey(res1[2], acron[0]);
			// Skip headers
			if (res1[2].equals("conference_location"))
				return;
			
			/*
			city.set(res[0]);	// Acronym
			word1.set(res[1]);	// Name
			word.set(res[2]);	// Location
	   		*/
			
			// output key-value pair
			context.write(word,one);
   		}
  	}
	public static class IntSumReducer
      		extends Reducer<CompositeGroupKey,IntWritable,CompositeGroupKey,IntWritable> {
   		private IntWritable result = new IntWritable();
		@Override
    		public void reduce(CompositeGroupKey key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
     	 		int sum = 0;
      			for (IntWritable val : values) {
        			//sum = sum.concat(val.toString());
				//sum = sum.concat("\n");
				sum += val.get();
      			}
      			result.set(sum);
      			context.write(key, result);
    		}
  	}
	public static void main(String[] args) throws Exception {
    		Configuration conf = new Configuration();
    		Job job = Job.getInstance(conf, "word count");
    		job.setJarByClass(WordCount.class);
    		job.setMapperClass(TokenizerMapper.class);
    		job.setCombinerClass(IntSumReducer.class);
    		job.setReducerClass(IntSumReducer.class);
    		job.setOutputKeyClass(CompositeGroupKey.class);
    		job.setOutputValueClass(IntWritable.class);
    		FileInputFormat.addInputPath(job, new Path(args[0]));
    		FileOutputFormat.setOutputPath(job, new Path(args[1]));
    		System.exit(job.waitForCompletion(true) ? 0 : 1);
  	}
}
