package org.apache.hama.semicluster.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.graph.Vertex;

/**
 * The SCMessage class defines the structure of the value stored by each vertex
 * in the graph Job which is same as the Message sent my each vertex.
 * 
 */
public class SCMessage implements WritableComparable<SCMessage> {
	private String semiClusterId;
	private double semiClusterScore;
	private List<Vertex<Text, DoubleWritable, SCMessage>> semiClusterVertexList = new ArrayList<Vertex<Text, DoubleWritable, SCMessage>>();
	private List<String> semiClusterContainThis = new ArrayList<String>();

	public SCMessage(String scId,
			List<Vertex<Text, DoubleWritable, SCMessage>> verticesEdges,
			double score) {
		this.semiClusterId = scId;
		this.semiClusterVertexList = verticesEdges;
		this.semiClusterScore = score;
	}

	public SCMessage(SCMessage msg) {
		this.semiClusterId = msg.getScId();
		for (Vertex<Text, DoubleWritable, SCMessage> v : msg.getVertexList())
			this.semiClusterVertexList.add(v);
		this.semiClusterScore = msg.getScore();
	}

	public SCMessage(List<String> semiClusterContainThis) {
		this.semiClusterId = "";
		this.semiClusterScore = 0.0;
		this.semiClusterVertexList = null;
		this.semiClusterContainThis = semiClusterContainThis;
	}

	public SCMessage() {
	}

	public double getScore() {
		return semiClusterScore;
	}

	public void setScore(double score) {
		this.semiClusterScore = score;
	}

	public List<Vertex<Text, DoubleWritable, SCMessage>> getVertexList() {
		return semiClusterVertexList;
	}

	public void addVertex(Vertex<Text, DoubleWritable, SCMessage> v) {
		this.semiClusterVertexList.add(v);
	}

	public String getScId() {
		return semiClusterId;
	}

	public void setScId(String scId) {
		this.semiClusterId = scId;
	}

	public void readFields(DataInput in) throws IOException {
		clear();
		String semiClusterId = in.readUTF();
		setScId(semiClusterId);
		double score = in.readDouble();
		setScore(score);
		if (in.readBoolean()) {
			int len = in.readInt();
			if (len > 0) {
				for (int i = 0; i < len; i++) {
					SemiClusteringVertex v = new SemiClusteringVertex();
					v.readFields(in);
					semiClusterVertexList.add(v);
				}
			}
		}
		int len = in.readInt();
		if (len > 0) {
			for (int i = 0; i < len; i++) {
				String clusterId = in.readUTF();
				semiClusterContainThis.add(clusterId);
			}
		}

	}

	private void clear() {
		semiClusterVertexList = new ArrayList<Vertex<Text, DoubleWritable, SCMessage>>();
		semiClusterContainThis = new ArrayList<String>();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(semiClusterId);
		out.writeDouble(semiClusterScore);

		if (this.semiClusterVertexList == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			out.writeInt(semiClusterVertexList.size());
			for (Vertex<Text, DoubleWritable, SCMessage> v : semiClusterVertexList) {
				v.write(out);
			}
		}
		out.writeInt(semiClusterContainThis.size());
		for (int i = 0; i < semiClusterContainThis.size(); i++) {
			out.writeUTF(semiClusterContainThis.get(i));

		}

	}

	public List<String> getSemiClusterContainThis() {
		return semiClusterContainThis;
	}

	public void setSemiClusterContainThis(List<String> semiClusterContainThis) {
		this.semiClusterContainThis = semiClusterContainThis;
	}

	public int compareTo(SCMessage m) {
		return (this.getScId().compareTo(m.getScId()));
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((semiClusterId == null) ? 0 : semiClusterId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SCMessage other = (SCMessage) obj;
		if (semiClusterId == null) {
			if (other.semiClusterId != null)
				return false;
		} else if (!semiClusterId.equals(other.semiClusterId))
			return false;
		return true;
	}
}
