package com.min.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.min.entity.Record;

public class MapReduce1 {

	public static class Map1 extends Mapper<Object, Text, Text, Record> {
		Text keys = new Text();

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Record>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			InputSplit inputSplit = context.getInputSplit();
			String name = ((FileSplit) inputSplit).getPath().getName();
			String[] split = value.toString().split("\t");
			Record record = new Record();
			record.setRecordId(split[0]);
			keys.set(split[0]);
			if ("record.txt".equals(name)) {
				record.setIndex(1);
			} else if ("reimburse.txt".equals(name)) {
				record.setIndex(2);
			}
			StringBuilder builder = new StringBuilder();
			for (int i = 1, len = split.length; i < len; i++) {
				builder.append(split[i]);
				builder.append(" ");
			}
			builder.deleteCharAt(builder.length() - 1);
			record.setValue(builder.toString());
			context.write(keys, record);
		}
	}

	public static class reduce1 extends Reducer<Text, Record, IntWritable, Text> {
		float sum = 0.0f;
		IntWritable iKey = new IntWritable();
		Text te = new Text();

		@Override
		protected void reduce(Text key, Iterable<Record> value,
				Reducer<Text, Record, IntWritable, Text>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			List<String> list1 = new ArrayList<String>();
			List<String> list2 = new ArrayList<String>();
			for (Record record : value) {
				if (record.getIndex() == 1) {
					int i = Integer.valueOf(record.getValue().split(" ")[4]);
					iKey.set(i);
					list1.add(record.getValue());
				}
				if (record.getIndex() == 2) {
					list2.add(record.getValue());
				}
			}
			for (String record1 : list1) {
				String string = "";
				for (String ss : list2) {
					String[] split = ss.split(" ");
					string = split[0];
					sum += Float.valueOf(split[1]);
				}
				te.set(key.toString() + " " + record1 + " " + string + " " + sum);
				context.write(iKey, te);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, "eee");
		job.setJarByClass(MapReduce1.class);

		job.setMapperClass(Map1.class);
		job.setReducerClass(reduce1.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Record.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		FileSystem hdfs = FileSystem.get(configuration);
		Path name = new Path("/thumbs/out1");
		if (hdfs.exists(name)) {
			hdfs.delete(name, true);
		}

		FileInputFormat.addInputPath(job, new Path("/thumbs/"));
		FileOutputFormat.setOutputPath(job, name);
		// 系统关闭与否
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
