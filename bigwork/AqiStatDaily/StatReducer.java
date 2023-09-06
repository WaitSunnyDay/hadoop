package com.aqi.AqiStatDaily;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StatReducer extends Reducer<Text, IntWritable, Text, Text> {

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1, Reducer<Text, IntWritable, Text, Text>.Context arg2)
			throws IOException, InterruptedException {
		int sum = 0;
		int aqi = 0;
		for (IntWritable arg : arg1) {
			aqi += arg.get();
			sum++;
		}
		String out = "每日空气质量指数：" + aqi / sum;
		arg2.write(arg0, new Text(out));
	}

}
