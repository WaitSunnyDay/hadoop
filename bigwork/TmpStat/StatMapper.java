package com.tmpstat.TmpStat;

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
		String[] info = data.split(",");
		if (key.get() != 0) {
			if (!info[5].equals("N/A")) {
				String time = info[0].substring(0, 6);
				int tmp = Integer.parseInt(info[5]);
				context.write(new Text(time), new IntWritable(tmp));
			}

		}
	}

}
