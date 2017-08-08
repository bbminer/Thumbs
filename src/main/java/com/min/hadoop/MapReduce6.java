package com.min.hadoop;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import com.min.entity.Record;

public class MapReduce6 {
	// 建表
	public static void createTable() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		Connection con = ConnectionFactory.createConnection(conf);
		HBaseAdmin admin = (HBaseAdmin) con.getAdmin();
		String tableName = "Thumbs";
		if (admin.tableExists(tableName) == false) {
			HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
			HColumnDescriptor columnDesc1 = new HColumnDescriptor("hospitalid");
			HColumnDescriptor columnDesc2 = new HColumnDescriptor("diseaseid");
			HColumnDescriptor columnDesc3 = new HColumnDescriptor("hospitalid+diseaseid");
			desc.addFamily(columnDesc1);
			desc.addFamily(columnDesc2);
			desc.addFamily(columnDesc3);
			admin.createTable(desc);
		}
	}

	public static class Map extends Mapper<Object, Text, Text, Record> {
		Text kText = new Text();

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Record>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] split = value.toString().split("\t");
			Record record = new Record();
			record.setRecordId(split[2] + "\t" + split[3]);
			kText.set(split[2] + "\t" + split[3]);

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

	public static class Reduce extends TableReducer<Text, Record, ImmutableBytesWritable> {
		@Override
		protected void reduce(Text arg0, Iterable<Record> arg1,
				Reducer<Text, Record, ImmutableBytesWritable, Mutation>.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// 住院
			int count1 = 0; // 总就诊人数
			float sumCost1 = 0.0f; // 总花费
			int sumRecovery1 = 0; // 总治愈人数
			int dayCount1 = 0; // 总住院天数
			float sumRecost1 = 0.0f; // 总报销

			// 门诊
			int count2 = 0; // 总就诊人数
			float sumCost2 = 0.0f; // 总花费
			int sumRecovery2 = 0; // 总治愈人数
			float sumRecost2 = 0.0f; // 总报销

			for (Record record : arg1) {
				String[] split = record.getValue().split("\t");

				// 住院
				if ("1".equals(split[5])) {
					count1++;
					sumCost1 += Float.valueOf(split[8]);
					sumRecovery1 += Integer.valueOf(split[9]);
					dayCount1 += DateCount(split[6], split[7]);
					sumRecost1 += Float.valueOf(split[11]);

				}
				// 门诊
				else if ("2".equals(split[5])) {
					count2++;
					sumCost2 += Float.valueOf(split[8]);
					sumRecovery2 += Integer.valueOf(split[9]);
					sumRecost2 += Float.valueOf(split[11]);
				}
			}
			Put put = new Put(arg0.getBytes());
			if (count1 > 0 && count2 == 0) {
				put.addColumn(Bytes.toBytes("hospitalid+diseaseid"), Bytes.toBytes("count"), Bytes.toBytes(count1));
				put.addColumn(Bytes.toBytes("hospitalid+diseaseid"), Bytes.toBytes("cost"), Bytes.toBytes(sumCost1));
				put.addColumn(Bytes.toBytes("hospitalid+diseaseid"), Bytes.toBytes("reimburse"),
						Bytes.toBytes(sumRecost1));
				put.addColumn(Bytes.toBytes("hospitalid+diseaseid"), Bytes.toBytes("recovery"),
						Bytes.toBytes(sumRecovery1));
				put.addColumn(Bytes.toBytes("hospitalid+diseaseid"), Bytes.toBytes("day"), Bytes.toBytes(dayCount1));
				arg2.write(new ImmutableBytesWritable(Bytes.toBytes("hospitalid+diseaseid")), put);
			}
			if (count2 > 0 && count1 == 0) {
				put.addColumn(Bytes.toBytes("hospitalid+diseaseid"), Bytes.toBytes("count"), Bytes.toBytes(count2));
				put.addColumn(Bytes.toBytes("hospitalid+diseaseid"), Bytes.toBytes("cost"), Bytes.toBytes(sumCost2));
				put.addColumn(Bytes.toBytes("hospitalid+diseaseid"), Bytes.toBytes("reimburse"),
						Bytes.toBytes(sumRecost2));
				put.addColumn(Bytes.toBytes("hospitalid+diseaseid"), Bytes.toBytes("recovery"),
						Bytes.toBytes(sumRecovery2));
				put.addColumn(Bytes.toBytes("hospitalid+diseaseid"), Bytes.toBytes("day"), Bytes.toBytes(0));
				arg2.write(new ImmutableBytesWritable(Bytes.toBytes("hospitalid+diseaseid")), put);
			}
			if (count1 > 0 && count2 > 0) {
				put.addColumn(Bytes.toBytes("hospitalid+diseaseid"), Bytes.toBytes("count"),
						Bytes.toBytes(count1 + count2));
				put.addColumn(Bytes.toBytes("hospitalid+diseaseid"), Bytes.toBytes("cost"),
						Bytes.toBytes(sumCost1 + sumCost2));
				put.addColumn(Bytes.toBytes("hospitalid+diseaseid"), Bytes.toBytes("reimburse"),
						Bytes.toBytes(sumRecost1 + sumRecost2));
				put.addColumn(Bytes.toBytes("hospitalid+diseaseid"), Bytes.toBytes("recovery"),
						Bytes.toBytes(sumRecovery1 + sumRecovery2));
				put.addColumn(Bytes.toBytes("hospitalid+diseaseid"), Bytes.toBytes("day"), Bytes.toBytes(dayCount1));
				arg2.write(new ImmutableBytesWritable(Bytes.toBytes("hospitalid+diseaseid")), put);
			}
		}

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
		createTable();
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		job.setJarByClass(MapReduce6.class);
		job.setMapperClass(Map.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Record.class);
		TableMapReduceUtil.initTableReducerJob("Thumbs", Reduce.class, job, null, null, null, null, false);
		FileInputFormat.addInputPath(job, new Path("/thumbs/out1/part-r-00000"));

		// 系统关闭与否
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
