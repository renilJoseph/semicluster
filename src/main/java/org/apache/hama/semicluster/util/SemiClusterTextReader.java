package org.apache.hama.semicluster.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;

/**
 * SemiClusterTextReader defines the way in which data is to be read from the
 * input file and store as a vertex with VertexId and Edges
 * 
 */
public class SemiClusterTextReader extends
		VertexInputReader<LongWritable, Text, Text, DoubleWritable, Text> {

	String lastVertexId = null;
	List<String> adjacents = new ArrayList<String>();

	@Override
	public boolean parseVertex(LongWritable key, Text value,
			Vertex<Text, DoubleWritable, Text> vertex) {

		String line = value.toString();
		String[] lineSplit = line.split("\t");
		if (!line.startsWith("#")) {
			if (lastVertexId == null) {
				lastVertexId = lineSplit[0];
			}
			if (lastVertexId.equals(lineSplit[0])) {
				adjacents.add(lineSplit[1]);
			} else {
				vertex.setVertexID(new Text(lastVertexId));
				for (String adjacent : adjacents) {
					String[] ValueSplit = adjacent.split("-");
					System.out.println(adjacent);
					vertex.addEdge(new Edge<Text, DoubleWritable>(new Text(
							ValueSplit[0]), new DoubleWritable(Double
							.parseDouble(ValueSplit[1]))));
				}
				adjacents.clear();
				lastVertexId = lineSplit[0];
				adjacents.add(lineSplit[1]);
				return true;
			}
		}
		return false;
	}

}