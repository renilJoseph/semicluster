package org.apache.hama.semicluster.hadoopjob;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ClusterGroupingReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String line = "";
		boolean count = false;
		for (Text val : values) {
			if (line.equals(""))
				line = val.toString();
			else {
				line = line + "," + val.toString();
				count = true;
			}
		}
		if(count)
		context.write(key, new Text(line));
	}
}
