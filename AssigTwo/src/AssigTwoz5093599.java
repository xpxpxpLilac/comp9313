import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


/**
 * COMP9313 Assignment2
 * @author z5093599
 * =======================
 * =     Algorithm       =
 * =======================
 *
 *	In the question, I used Breadth-first Search to find the shortest path from the starting node.
 *	It starts from the starting node and expand to its adjacent neighbors through the iterations. 
 *	Every time we find a new path that is shorter than current path to the specific node, 
 *	we update it and set the updated flag to true. In the next iteration, the nodes that are updated 
 *	in the previous round will be expanded again until no more node are updated in the end.
 *
 *	======================
 *	=	Data Structure   =
 *	======================
 *
 *	There are tow new data structures in this program. 
 *	1. Path
 *		Path is used to show the path from starting node to a specific node. 
 *		For example, starting from N0, for N2, it may look like N0-N3-N2.
 *		We can update the path by using the function addNode(node) in this class.
 *	2. BfsRecordInfo
 *		BfsRecordInfo is used to store the node data through the BFS process. 
 *		It looks like (6,[(N1:4),(N2:3)],false,N3-N4-N0)
 *		where 6 is the distance from starting node to this node, 
 *		[(N1:4),(N2:3)] is the adjacent neighbors and corresponding distance to their neighbors,
 *		false is the updated flag, it indicates whether the node has been updated in the previous iteration,
 *		N3-N4-N0 is the current path to this node.
 *	
 *	======================
 *	=       Example      =
 *	======================
 *	In this section, the name of each variable is exactly the same as it in the code. 
 *	For example, in the sample input and N0 as the starting node:
 *	N0,N1,4
 *  N0,N2,3
 *  N1,N2,2
 *  N1,N3,2
 *  N2,N3,7
 *  N3,N4,2
 *  N4,N0,4
 *  N4,N1,4
 *  N4,N5,6
 *  
 *  We first proceed the data into format, given the name "prep1" 
 *  (N0,(N1,4))
 *  (N0,(N2,3))
 *  (N1,(N2,2))
 *  (N1,(N3,2))
 *  (N2,(N3,7))
 *  (N3,(N4,2))
 *  (N4,(N0,4))
 *  (N4,(N1,4))
 *  (N4,(N5,6))
 *  
 *  After grouping by key and proceeding it to BfsRecordInfo structure, then we have "prep2"
 *  We set the distance from N0 to N0 as 0 and since we have no idea of the distance from N0 to other nodes, we set it to -1.
 *  Since N0 is the starting node and when the iteration starts, it should be expanded, we set the N0 flag to true.
 *  (N0,(0,[(N1:4),(N2:3)],true,N0))
 *  (N1,(-1,[(N2:2),(N3:2)],false,N1))
 * 	(N3,(-1,[(N4:2)],false,N3))
 *  (N4,(-1,[(N0:4),(N1:4),(N5:6)],false,N4))
 *  (N2,(-1,[(N3:7)],false,N2))
 *	
 *	Then we start a while loop to start the expanding iterations and store the result of each iteration to variable "result".
 *	"emitNeighborNodes" is the data we get after one mapping, that is expanding the neighbor nodes of the updated node in the previous round.
 *	Since we cannot know the adjacent neighbor info when we expand, like in (N0,(0,[(N1:4),(N2:3)],true,N0)), when we expand to N1, 
 *	we don't know other information about N1 beside the total distance, so the emitted new data look like (N1,(4,[],false,N0-N1))
 *	(N1,(4,[],false,N0-N1))
 *	(N2,(3,[],false,N0-N2))
 *	(N0,(0,[(N1:4),(N2:3)],false,N0))
 *	(N1,(-1,[(N2:2),(N3:2)],false,N1))
 *	(N3,(-1,[(N4:2)],false,N3))
 *	(N4,(-1,[(N0:4),(N1:4),(N5:6)],false,N4))
 *	(N2,(-1,[(N3:7)],false,N2))
 *	Then we reduce by key and copy the adjacent neighbor data to the latest node BfsRecordInfo
 *	(N0,(0,[(N1:4),(N2:3)],false,N0))
 *	(N1,(4,[(N2:2),(N3:2)],true,N0-N1))
 *	(N3,(-1,[(N4:2)],false,N3))
 *	(N4,(-1,[(N0:4),(N1:4),(N5:6)],false,N4))
 *	(N2,(3,[(N3:7)],true,N0-N2))
 *	Here, one iteration is finished and we can see that N1 and N2 are updated and the flags are set to true, others are false.
 *	
 *	In the following iterations, we just expand the nodes that with true flag and loop until no node is with true flag.
 *	Since only when a shorter path occurs to a node, the node can be updated, when no node is updated in the previous round,
 *	that means for each node the path is the shortest one.
 *	
 */


public class AssigTwoz5093599 {
	
	/**
	 * 
	 * @author z5093599
	 *
	 *	This class is to construct the path to each node
	 *	It has two constructor, one with parameter, one without
	 *	Function addNode is used to append node to the current path
	 *
	 */
	public static class Path implements Serializable {
			
		Iterable<String> path;
		
		public Path() {
			ArrayList<String> lists = new ArrayList<String>();
			this.path = lists;
		}
		
		public Path(Iterable<String> path) {
			super();
			this.path = path;
		}

		public void addNode(String node) {
			ArrayList<String> lists = new ArrayList<String>();
			path.forEach(lists::add);
			lists.add(node);
			this.path = lists;
		}
		
		
		public String toString() {
			
			StringBuilder sb = new StringBuilder();
			ArrayList<String> lists = new ArrayList<String>();
			path.forEach(lists::add);
			for(int i=0; i< lists.size(); i++) {
				sb.append(lists.get(i)).append('-');
			}
			if (lists.size() > 0) {sb.setLength(sb.length() - 1);}
			
			return sb.toString();
		}
		
	}
	

	/**
	 * 
	 * @author z5093599
	 *
	 *	This class is used to store BFS data
	 *	dist => current shortest distance
	 *	graph => adjacent neighbors and corresponding distance
	 *	updated => the updated flag, to show whether the node has been updated in the previous iteration
	 *	
	 */
	
	public static class BfsRecordInfo implements Serializable {
			
		Integer dist;
		Iterable<Tuple2<String,Integer>> graph;
		Boolean updated;
		Path path;

		
		public BfsRecordInfo() {
			super();
			this.dist = -1;
			ArrayList<Tuple2<String,Integer>> graphArray = new ArrayList<Tuple2<String,Integer>>();
			this.graph = graphArray;
			this.updated = false;
			this.path = new Path();
		}

		public BfsRecordInfo(Integer dist, Iterable<Tuple2<String, Integer>> graph, Boolean updated, Path path) {
			super();
			this.dist = dist;
			this.graph = graph;
			this.updated = updated;
			this.path = path;
		}


		public Integer getDist() {
			return dist;
		}


		public void setDist(Integer dist) {
			this.dist = dist;
		}


		public Iterable<Tuple2<String, Integer>> getGraph() {
			return graph;
		}


		public void setGraph(Iterable<Tuple2<String, Integer>> graph) {
			this.graph = graph;
		}


		public Boolean getUpdated() {
			return updated;
		}



		public void setUpdated(Boolean updated) {
			this.updated = updated;
		}



		public Path getPath() {
			return path;
		}



		public void setPath(Path path) {
			this.path = path;
		}



		public String toString() {
			
			StringBuilder sb = new StringBuilder();
			sb.append('(').append(dist).append(',').append('[');
			ArrayList<Tuple2<String, Integer>> lists = new ArrayList<Tuple2<String, Integer>>();
			graph.forEach(lists::add);
			for(int i=0; i< lists.size(); i++) {
				sb.append('(').append(lists.get(i)._1).append(':').append(lists.get(i)._2).append(')').append(',');
			}
			if (lists.size() > 0) {sb.setLength(sb.length() - 1);}
			sb.append(']').append(',').append(updated).append(',').append(path).append(')');
			return sb.toString();
		}
		
	}
	
	/**
	 * 
	 * @author z5093599
	 *
	 *	Used to put node with distance -1 to the end
	 *	after BFS is finished
	 */
	
	public static class CompareNegative implements Comparator<Integer>, Serializable  {

		@Override
		public int compare(Integer input1, Integer input2) {
			// TODO Auto-generated method stub
			Integer result = 0;
			if (input1 > 0 && input2 > 0) {
				if (input1 > input2) {
					result = 1;
				} else {
					result = -1;
				}
			} else if (input1 > 0 && input2 < 0) {
				result = -1;
			} else if (input1 < 0 && input2 > 0) {
				result = 1;
			}
			
			return result;
		}
		
	}
	
	/**
	 * 
	 * @author z5093599
	 *
	 *	Help to expand the neighbor nodes when the updated flag is set to true
	 *	Pass down the original info is the flag is set to false
	 *
	 */
	
	public static class EmitMapper implements PairFlatMapFunction<Tuple2<String,BfsRecordInfo>, String, BfsRecordInfo> {

		@Override
		public Iterator<Tuple2<String, BfsRecordInfo>> call(Tuple2<String, BfsRecordInfo> input) throws Exception {
			
			BfsRecordInfo record = input._2;
			Iterable<Tuple2<String,Integer>> graph = record.graph;
			Boolean update = record.updated;
			Integer dist = record.dist;
	        
			ArrayList<Tuple2<String, BfsRecordInfo>> ret = new ArrayList<Tuple2<String, BfsRecordInfo>>();

			if (update) {
				
				input._2.setUpdated(false);
				
				ArrayList<Tuple2<String,Integer>> conns = new ArrayList<>();
				graph.forEach(conns::add);

				for(int i=0; i< conns.size(); i++) {

					if (record.dist >= 0) {
						String node = conns.get(i)._1;
						Integer distance = conns.get(i)._2 + dist;
						Path nodePath = new Path(record.path.path);
						nodePath.addNode(node);
						BfsRecordInfo four = new BfsRecordInfo();
						four.setDist(distance);
						four.setPath(nodePath);
						ret.add(new Tuple2<>(node, four));
					}
				}
				
			} 
			
			ret.add(input);
			
			return ret.iterator();
		}
	}

	
	/**
	 * 
	 * @author z5093599
	 *
	 *	reduce the records by key, pass the adjacent neighbor information to the current shortest record of corresponding node
	 *
	 */
	
	public static class EmitReducer implements Function2<BfsRecordInfo, BfsRecordInfo, BfsRecordInfo> {

		
		@Override
		public BfsRecordInfo call(BfsRecordInfo input1, BfsRecordInfo input2) throws Exception {
			
			BfsRecordInfo record = input1;
			Iterable<Tuple2<String,Integer>> graph1 = input1.getGraph();
			Iterable<Tuple2<String,Integer>> graph2 = input2.getGraph();

			int size1 = 0;
			for(Tuple2<String,Integer> value : graph1) { size1++; }
			
			int size2 = 0;
			for(Tuple2<String,Integer> value : graph2) { size2++; }
			

			if (size1 > 0) { 
				if (input1.getDist() > 0 && input2.getDist() > 0) {
					if (input1.getDist() > input2.getDist()) {
						input2.setGraph(graph1);
						input2.setUpdated(true);
						record = input2;
					}
				} else if (input1.getDist() < 0 && input2.getDist() > 0) {
					input2.setGraph(graph1);
					input2.setUpdated(true);
					record = input2;
				}
			} else if (size2 > 0) {
				if (input1.getDist() > 0 && input2.getDist() > 0) {
					if (input2.getDist() > input1.getDist()) {
						input1.setGraph(graph2);
						input1.setUpdated(true);
						record = input1;
					} else {
						record = input2;
					}
				} else if (input1.getDist() > 0 && input2.getDist() < 0) {
					input1.setGraph(graph2);
					input1.setUpdated(true);
					record = input1;
				}
			} else {
				if (input1.getDist() > input2.getDist()) {
					record = input2;
				} else {
					record = input1;
				}
			}
			
			
			return record;
		}
		
	}
	
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
				.setMaster("local")
				.setAppName("Assignment 2");
		
		JavaSparkContext context = new JavaSparkContext(conf);
		
		JavaRDD<String> input = context.textFile(args[1]);
		
		// refer to line 62
		JavaPairRDD<String, Tuple2<String, Integer>> prep1 = input.mapToPair(new PairFunction<String, String, Tuple2<String, Integer>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Tuple2<String, Integer>> call(String line) throws Exception {
				
				String [] parts = line.split(",");
				String startNode = parts[0];
				String endNode = parts[1];
				Integer weight = Integer.parseInt(parts[2]);
				
				return new Tuple2<String, Tuple2<String,Integer>>(startNode, new Tuple2<>(endNode, weight));
			}

		});
		
		//refer to line 73
		JavaPairRDD<String, BfsRecordInfo> prep2 = prep1.groupByKey().mapToPair(
									new PairFunction<Tuple2<String,Iterable<Tuple2<String,Integer>>>, String, BfsRecordInfo>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, BfsRecordInfo> call(Tuple2<String, Iterable<Tuple2<String, Integer>>> input) throws Exception {
				
				String key = input._1;
				BfsRecordInfo record;
				Path path = new Path();
				path.addNode(key);
				
				if (key.equals(args[0])) {
					
					record = new BfsRecordInfo(0, input._2, true, path);
				} else {
					record = new BfsRecordInfo(-1, input._2, false, path);
				}
				
				return new Tuple2<>(key, record);
			}
		});

		JavaPairRDD<String, BfsRecordInfo> result = prep2;
		
		long counter = 1;

		while (counter > 0) {
			
			JavaPairRDD<String, BfsRecordInfo> emitNeighborNodes = result.flatMapToPair(new EmitMapper());
			
			JavaPairRDD<String, BfsRecordInfo> upDateToShortest = emitNeighborNodes.reduceByKey(new EmitReducer());			
			
			// check whether there are updated flags setting to true
			counter = upDateToShortest.filter(k -> (k._2.updated == true)).count(); 
			
			result = upDateToShortest;
			
		}
		
		
		JavaPairRDD<Integer,Tuple2<String, Path>> sortByDist = result
								.filter(k -> (!k._1.equals(args[0])))
								.mapToPair(
				new PairFunction<Tuple2<String,BfsRecordInfo>, Integer, Tuple2<String, Path> >() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, Tuple2<String, Path>> call(Tuple2<String, BfsRecordInfo> input) throws Exception {
						
						String node = input._1;
						BfsRecordInfo record = input._2;
						Integer key = record.getDist();
						Path path = record.getPath();
						
						return new Tuple2<Integer, Tuple2<String,Path>>(key, new Tuple2<String,Path>(node, path));
					}
						
		}).sortByKey(new CompareNegative());
		
		JavaRDD<String> finalResult = sortByDist.map(new Function<Tuple2<Integer,Tuple2<String,Path>>, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<Integer, Tuple2<String, Path>> input) throws Exception {

				Integer dist = input._1;
				String node = input._2._1;
				Path path = input._2._2;
				
				if (dist == -1) {
					path = new Path();
				}
				
				String result = node + "," + dist + "," + path;
				return result;
			}
		});
		finalResult.saveAsTextFile(args[2]);
	}

}
