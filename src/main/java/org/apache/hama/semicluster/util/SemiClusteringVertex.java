package org.apache.hama.semicluster.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.Vertex;

/**
 * SemiClusteringVertex Class defines each vertex in a Graph job and the
 * compute() method is the function which is applied on each Vertex in the graph
 * on each Super step of the job execution.
 * 
 */
public class SemiClusteringVertex extends
		Vertex<Text, DoubleWritable, SCMessage> {
	private int semiClusterMaximumVertexCount;
	private int graphJobMessageSentCount;

	@Override
	public void setup(Configuration conf) {
		semiClusterMaximumVertexCount = conf.getInt(
				"semicluster.max.vertex.count", 10);
		graphJobMessageSentCount = conf.getInt(
				"semicluster.max.message.sent.count", 10);
	}

	/**
	 * The user overrides the Compute() method, which will be executed at each
	 * active vertex in every superstep
	 */
	@Override
	public void compute(Iterable<SCMessage> messages) throws IOException {
		if (this.getSuperstepCount() == 0) {
			firstSuperStep();
		}
		if (this.getSuperstepCount() >= 1) {
			Set<SCMessage> scListContainThis = new TreeSet<SCMessage>();
			Set<SCMessage> scListNotContainThis = new TreeSet<SCMessage>();
			List<SCMessage> scList = new ArrayList<SCMessage>();
			for (SCMessage msg : messages) {
				if (!isVertexInSc(msg)) {
					scListNotContainThis.add(msg);
					SCMessage msgNew = new SCMessage(msg);
					msgNew.addVertex(this);
					msgNew.setScId("C"
							+ createNewSemiClusterName(msgNew.getVertexList()));
					msgNew.setScore(semiClusterScoreCalcuation(msgNew));
					scListContainThis.add(msgNew);
				} else {
					scListContainThis.add(msg);
				}
			}
			scList.addAll(scListContainThis);
			scList.addAll(scListNotContainThis);
			sendBestSCMsg(scList);
			updatesVertexSemiClustersList(scListContainThis);
		}
	}

	public List<SCMessage> addSCList(List<SCMessage> scList, SCMessage msg) {
		return scList;
	}

	/**
	 * This function create a new Semi-cluster ID for a semi-cluster from the
	 * list of vertices in the cluster.It first take all the vertexIds as a list
	 * sort the list and then find the HashCode of the Sorted List.
	 */
	public int createNewSemiClusterName(
			List<Vertex<Text, DoubleWritable, SCMessage>> semiClusterVertexList) {
		List<String> vertexIDList = getSemiClusterVerticesIdList(semiClusterVertexList);
		Collections.sort(vertexIDList);
		return (vertexIDList.hashCode());
	}

	/**
	 * Function which is executed in the first SuperStep
	 * 
	 * @throws IOException
	 */
	public void firstSuperStep() throws IOException {
		Vertex<Text, DoubleWritable, SCMessage> v = this;
		List<Vertex<Text, DoubleWritable, SCMessage>> lV = new ArrayList<Vertex<Text, DoubleWritable, SCMessage>>();
		lV.add(v);
		SCMessage initialClusters = new SCMessage("C"
				+ createNewSemiClusterName(lV), lV, 1);
		sendMessageToNeighbors(initialClusters);
		this.setValue(null);
	}

	/**
	 * Vertex V updates its list of semi-clusters with the semi- clusters from
	 * c1 , ..., ck , c'1 , ..., c'k that contain V
	 */
	public void updatesVertexSemiClustersList(Set<SCMessage> scListContainThis)
			throws IOException {
		List<String> scList = new ArrayList<String>();
		Set<SCMessage> sortedSet = new TreeSet<SCMessage>(
				new Comparator<SCMessage>() {

					@Override
					public int compare(SCMessage o1, SCMessage o2) {
						return (o1.getScore() == o2.getScore() ? 0
								: o1.getScore() < o2.getScore() ? -1 : 1);
					}
				});
		sortedSet.addAll(scListContainThis);
		int count = 0;
		for (SCMessage msg : sortedSet) {
			scList.add(msg.getScId());
			if (count > graphJobMessageSentCount)
				break;
		}
		SCMessage vertexValue = new SCMessage(scList);
		this.setValue(vertexValue);
	}

	/**
	 * Function to calcualte the Score of a semi-cluster
	 * 
	 * @param message
	 * @return
	 */
	public double semiClusterScoreCalcuation(SCMessage message) {
		double iC = 0.0, bC = 0.0, fB = 1.0, sC = 0.0;
		int vC = 0;
		List<String> vertexId = getSemiClusterVerticesIdList(message
				.getVertexList());
		vC = vertexId.size();
		for (Vertex<Text, DoubleWritable, SCMessage> v : message
				.getVertexList()) {
			List<Edge<Text, DoubleWritable>> eL = v.getEdges();
			for (Edge<Text, DoubleWritable> e : eL) {
				if (vertexId.contains(e.getDestinationVertexID().toString())
						&& e.getCost() != null) {
					iC = iC + e.getCost().get();
				} else if (e.getCost() != null) {
					bC = bC + e.getCost().get();
				}
			}
		}
		if (vC > 1)
			sC = (iC - fB * bC) / ((vC * (vC - 1)) / 2);
		return sC;
	}

	/**
	 * Returns a Array List of vertexIds from a List of Vertex<Text,
	 * DoubleWritable, SCMessage> Objects
	 * 
	 * @param lV
	 * @return
	 */
	public List<String> getSemiClusterVerticesIdList(
			List<Vertex<Text, DoubleWritable, SCMessage>> lV) {
		Iterator<Vertex<Text, DoubleWritable, SCMessage>> vertexItrator = lV
				.iterator();
		List<String> vertexId = new ArrayList<String>();
		while (vertexItrator.hasNext()) {
			vertexId.add(vertexItrator.next().getVertexID().toString());
		}
		return vertexId;
	}

	/**
	 * If a semi-cluster c does not already contain V , and Vc < Mmax , then V
	 * is added to c to form c' .
	 */
	public boolean isVertexInSc(SCMessage msg) {
		List<String> vertexId = getSemiClusterVerticesIdList(msg
				.getVertexList());
		if (vertexId.contains(this.getVertexID().toString())
				&& vertexId.size() < semiClusterMaximumVertexCount)
			return true;
		else
			return false;
	}

	/**
	 * The semi-clusters c1 , ..., ck , c'1 , ..., c'k are sorted by their
	 * scores, and the best ones are sent to V ’s neighbors.
	 */
	public void sendBestSCMsg(List<SCMessage> scList) throws IOException {
		Collections.sort(scList, new Comparator<SCMessage>() {

			@Override
			public int compare(SCMessage o1, SCMessage o2) {
				return (o1.getScore() == o2.getScore() ? 0 : o1.getScore() < o2
						.getScore() ? -1 : 1);
			}
		});
		Iterator<SCMessage> scItr = scList.iterator();
		int count = 0;
		while (scItr.hasNext()) {
			sendMessageToNeighbors(scItr.next());
			count++;
			if (count > graphJobMessageSentCount)
				break;
		}
	}
}
