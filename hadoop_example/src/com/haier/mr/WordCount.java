package com.haier.mr;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
	public static void main(String[] args) throws Exception {
		args = new String[2];
		args[0] = "input";
		args[1] = "output";
		Configuration conf = new Configuration();
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		String[] otherArgs = gop.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			System.out.println("reduce key:" + key + " context:" + context);
			int sum = 0;
			System.out.println(" for (IntWritable val : values)");
			for (IntWritable val : values) {
				System.out.println(" val:" + val);
				sum += val.get();
			}
			System.out.println("}\n this.result.set(sum);  sum is :" + sum + " \n context.write(key, this.result);");
			this.result.set(sum);
			context.write(key, this.result);
		}
	}

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private static final IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			System.out.println("map key:" + key + " value:" + value + " context:" + context);
			StringTokenizer itr = new StringTokenizer(value.toString());
			System.out.println("while(){");
			while (itr.hasMoreTokens()) {
				String str = itr.nextToken();
				System.out.println("this.word.set(str):" + str + " context.write(this.word, one);");
				this.word.set(str);
				context.write(this.word, one);
			}

			System.out.println("}");
		}
	}
}

/*
 * Location: C:\Users\admin\Desktop\新建文件夹\hadoop-examples-1.0.3\ Qualified Name:
 * org.apache.hadoop.examples.WordCount JD-Core Version: 0.6.0
 */