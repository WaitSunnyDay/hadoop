package com.aqi.AqiStat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StatMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String data = value.toString();
		String[] info = data.split("\\D+");
		int aqi = Integer.parseInt(info[1]);
		String str = "";
		if (aqi <= 50) {
			str = "优";
		} else if (aqi <= 100) {
			str = "良";
		} else if (aqi <= 150) {
			str = "轻度污染";
		} else if (aqi <= 200) {
			str = "中度污染";
		} else if (aqi <= 300) {
			str = "重度污染";
		} else {
			str = "严重污染";
		}
		context.write(new Text(str), new IntWritable(1));
	}

}
