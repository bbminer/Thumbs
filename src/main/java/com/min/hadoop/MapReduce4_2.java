package com.min.hadoop;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.min.entity.Record;

public class MapReduce4_2 {
	public static class Map extends Mapper<Object, Text, Text, Record> {
		Text kText = new Text();

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Record>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] split = value.toString().split("\t");
			if (split[0].equals("1")) {
				Record record = new Record();
				record.setRecordId(split[2]);
				kText.set(split[2]);

				// 遍历value
				StringBuilder builder = new StringBuilder();
				for (int i = 1, len = split.length; i < len; i++) {
					builder.append(split[i]);
					builder.append("\t");
				}
				builder.deleteCharAt(builder.length() - 1);

				record.setValue(builder.toString());
				context.write(kText, record);
			}
		}
	}

	public static class Reduce extends Reducer<Text, Record, Text, Text> {
		Text vText = new Text();

		@Override
		protected void reduce(Text arg0, Iterable<Record> arg1, Reducer<Text, Record, Text, Text>.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int count = 0; // 总就诊人数
			float sumCost = 0.0f; // 总花费
			int sumRecovery = 0; // 总治愈人数
			int dayCount = 0; // 总住院天数
			float sumRecost = 0.0f; // 总报销
			for (Record record : arg1) {
				String[] split = record.getValue().split("\t");
				count++;
				sumCost += Float.valueOf(split[8]);
				sumRecovery += Integer.valueOf(split[9]);
				dayCount += DateCount(split[6], split[7]);
				sumRecost += Float.valueOf(split[11]);
			}
			String avgRecovery = String.format("%.4f%%", sumRecovery / (count * 1.0f) * 100); // 治愈率
			String avgCost = String.format("%.4f%%", sumRecost / (sumCost * 1.0f) * 100);// 人均报销比例
			vText.set(count + "\t" + sumCost / count + "\t" + sumRecost / count + "\t" + avgCost + "\t"
					+ dayCount / count + "\t" + avgRecovery);
			arg2.write(arg0, vText);
		}

		// 计算住院天数
		private int DateCount(String startTime, String endTime) {
			SimpleDateFormat sFormat = new SimpleDateFormat("yyyy-MM-dd");
			try {
				long tmp = sFormat.parse(endTime).getTime() - sFormat.parse(startTime).getTime();
				return (int) (tmp / (1000 * 24 * 60 * 60));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return 0;
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		job.setJarByClass(MapReduce4_2.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Record.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileSystem hdfs = FileSystem.get(configuration);
		Path name = new Path("/thumbs/out2");
		if (hdfs.exists(name)) {
			hdfs.delete(name, true);
		}

		FileInputFormat.addInputPath(job, new Path("/thumbs/out1/part-r-00000"));
		FileOutputFormat.setOutputPath(job, name);
		// 系统关闭与否
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
