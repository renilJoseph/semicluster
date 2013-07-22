package org.apache.hama.semicluster.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.graph.GraphJobMessage;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexOutputWriter;

/**
 * The VertexOutputWriter defines what parts of the vertex shall be written to
 * the output format.
 * 
 * @param <V>
 *            the vertexID type.
 * @param <E>
 *            the edge value type.
 * @param <M>
 *            the vertex value type.
 */
@SuppressWarnings("rawtypes")
public class SemiClusterVertexOutputWriter<KEYOUT extends Writable, VALUEOUT extends Writable, V extends WritableComparable, E extends Writable, M extends Writable>
		implements VertexOutputWriter<KEYOUT, VALUEOUT, V, E, M> {

	@Override
	public void setup(Configuration conf) {
		// do nothing
	}

	@SuppressWarnings("unchecked")
	@Override
	public void write(Vertex<V, E, M> vertex,
			BSPPeer<Writable, Writable, KEYOUT, VALUEOUT, GraphJobMessage> peer)
			throws IOException {
		SCMessage vertexValue = (SCMessage) vertex.getValue();
		peer.write((KEYOUT) vertex.getVertexID(), (VALUEOUT) new Text(
				vertexValue.getSemiClusterContainThis().toString()));
	}

}
