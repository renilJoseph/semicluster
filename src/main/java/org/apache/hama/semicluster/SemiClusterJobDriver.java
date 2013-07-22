package org.apache.hama.semicluster;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.semicluster.util.SCMessage;
import org.apache.hama.semicluster.util.SemiClusterTextReader;
import org.apache.hama.semicluster.util.SemiClusterVertexOutputWriter;
import org.apache.hama.semicluster.util.SemiClusteringVertex;

public class SemiClusterJobDriver {

	protected static final Log LOG = LogFactory.getLog(SemiClusterJobDriver.class);
	private static final String outputPathString = "semicluster.outputpath";
	private static final String inputPathString = "semicluster.inputmatrixpath";
	private static final String requestedGraphJobMaxIterationString = "hama.graph.max.iteration";
	private static final String semiClusterMaximumVertexCount = "semicluster.max.vertex.count";
	private static final String graphJobMessageSentCount = "semicluster.max.message.sent.count";

	public static void startTask(HamaConfiguration conf) throws IOException,
			InterruptedException, ClassNotFoundException {
		GraphJob semiClusterJob = new GraphJob(conf, SemiClusterJobDriver.class);
		semiClusterJob
				.setVertexOutputWriterClass(SemiClusterVertexOutputWriter.class);
		semiClusterJob.setJobName("SemiClusterJob");
		semiClusterJob.setVertexClass(SemiClusteringVertex.class);
		semiClusterJob.setInputPath(new Path(conf.get(inputPathString)));
		semiClusterJob.setOutputPath(new Path(conf.get(outputPathString)));

		semiClusterJob.set("hama.graph.self.ref", "true");
		semiClusterJob.set("hama.graph.repair", "true");

		semiClusterJob.setVertexIDClass(Text.class);
		semiClusterJob.setVertexValueClass(SCMessage.class);
		semiClusterJob.setEdgeValueClass(DoubleWritable.class);

		semiClusterJob.setInputKeyClass(LongWritable.class);
		semiClusterJob.setInputValueClass(Text.class);
		semiClusterJob.setInputFormat(TextInputFormat.class);
		semiClusterJob.setVertexInputReaderClass(SemiClusterTextReader.class);

		semiClusterJob.setPartitioner(HashPartitioner.class);

		semiClusterJob.setOutputFormat(TextOutputFormat.class);
		semiClusterJob.setOutputKeyClass(Text.class);
		semiClusterJob.setOutputValueClass(Text.class);

		long startTime = System.currentTimeMillis();
		if (semiClusterJob.waitForCompletion(true)) {
			System.out.println("Job Finished in "
					+ (System.currentTimeMillis() - startTime) / 1000.0
					+ " seconds");
		}
	}

	private static void printUsage() {
		LOG.info("Usage: SemiClusterO <input path>  <output path> [number of tasks (default max)]");
	}

	/**
	 * Function parses command line in standart form.
	 */
	private static void parseArgs(HamaConfiguration conf, String[] args) {
		if (args.length < 2) {
			printUsage();
			System.exit(-1);
		}

		conf.set(inputPathString, args[0]);

		Path path = new Path(args[1]);
		conf.set(outputPathString, path.toString());

		if (args.length >= 3) {
			try {
				int taskCount = Integer.parseInt(args[2]);
				if (taskCount < 0) {
					printUsage();
					throw new IllegalArgumentException(
							"The number of requested job maximum iteration count can't be negative. Actual value: "
									+ String.valueOf(taskCount));
				}
				conf.setInt(requestedGraphJobMaxIterationString, taskCount);
				if (args.length >= 4) {
					int maximumVertexCount = Integer.parseInt(args[3]);
					if (maximumVertexCount < 0) {
						printUsage();
						throw new IllegalArgumentException(
								"The number of  maximum vertex  count can't be negative. Actual value: "
										+ String.valueOf(maximumVertexCount));
					}
					conf.setInt(semiClusterMaximumVertexCount,
							maximumVertexCount);
					if (args.length == 5) {
						int messageSentCount = Integer.parseInt(args[4]);
						if (messageSentCount < 0) {
							printUsage();
							throw new IllegalArgumentException(
									"The number of  maximum message sent count can't be negative. Actual value: "
											+ String.valueOf(messageSentCount));
						}
						conf.setInt(graphJobMessageSentCount, messageSentCount);

					}
				}
			} catch (NumberFormatException e) {
				printUsage();
				throw new IllegalArgumentException(
						"The format of job maximum iteration count is int. Can not parse value: "
								+ args[2]);
			}
		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		HamaConfiguration conf = new HamaConfiguration();
		parseArgs(conf, args);
		startTask(conf);
	}
}
