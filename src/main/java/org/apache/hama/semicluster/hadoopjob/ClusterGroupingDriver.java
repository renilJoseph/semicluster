package org.apache.hama.semicluster.hadoopjob;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hama.semicluster.SemiClusterJobDriver;

public class ClusterGroupingDriver {
	protected static final Log LOG = LogFactory.getLog(SemiClusterJobDriver.class);

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			printUsage();
			System.exit(-1);
		}
		String cdrDataInputPath = args[0];
		String cdrDataTempOutputPath = args[1];
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Cluster Grouping Job");
		job.setJarByClass(ClusterGroupingDriver.class);
		job.setMapperClass(ClusterGroupingMapper.class);
		job.setReducerClass(ClusterGroupingReducer.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(cdrDataTempOutputPath));
		FileInputFormat.addInputPath(job, new Path(cdrDataInputPath));
		FileOutputFormat.setOutputPath(job, new Path(cdrDataTempOutputPath));
		job.waitForCompletion(true);
	}

	private static void printUsage() {
		LOG.info("Usage: ClusterGroupingDriver <input path>  <output path>");
	}
}
