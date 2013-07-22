package org.apache.hama.semicluster.hadoopjob;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ClusterGroupingMapper extends Mapper<Text, Text, Text, Text> {
	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {
		if (value.toString().startsWith("[")) {
		    String line = value.toString().replaceAll("[\\[\\] ]", "");
			StringTokenizer st = new StringTokenizer(line, ",");
			while (st.hasMoreTokens())
				context.write(new Text(st.nextToken()), key);
		}
	}
}
