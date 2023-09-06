package com.tmpstat.TmpStat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StatReducer extends Reducer<Text, IntWritable, Text, Text> {

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1, Reducer<Text, IntWritable, Text, Text>.Context arg2)
			throws IOException, InterruptedException {
		int sum = 0;
		int tmp = 0;
		for (IntWritable arg : arg1) {
			tmp += arg.get();
			sum++;
		}
		String out = "月平均气温：" + tmp / sum;
		arg2.write(arg0, new Text(out));
	}

}
