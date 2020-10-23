//Author : Chang Wang
//Finished in: 18/11/2019
//in this assignment I used spark to implement solution
//there is no error in my code and it could obtain the correct output
//the output is formatted correctly as described, and it has been sorted by length
//N2,3,N0-N2
//N1,4,N0-N1
//N3,6,N0-N1-N3
//N4,8,N0-N1-N3-N4
//N5,14,N0-N1-N3-N4-N5
//I searched for some web page to help me understand and learn more, here are the URL of resources:
//https://blog.csdn.net/xiefu5hh/article/details/51782319
//https://blog.csdn.net/qq_32023541/article/details/79239536
//https://www.runoob.com/java/java-tutorial.html
//https://www.cnblogs.com/bigmall/p/7123361.html

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;


public class AssigTwoz5196324 {
	
	public static JavaPairRDD<String, Tuple3<Integer, Path, HashMap<String, Integer>>> 
	BFS(JavaPairRDD<String, Tuple3<Integer, Path, HashMap<String, Integer>>> inputRdd) {
		//for each node, I could find all neighbor nodes of it.
		//nodeNumber,<distance, path, HashMap>	
		//for original node we should also compare itself with its neighbor.
		JavaPairRDD <String, Tuple3<Integer, Path, HashMap<String, Integer>>> map1 = 
		inputRdd.flatMapToPair(line -> {
			//initial the list
			List<Tuple2<String, Tuple3<Integer, Path, HashMap<String, Integer>>>> result = new ArrayList<>();
			for ( Map.Entry e: line._2._3().entrySet()) {
				String neighborNode = e.getKey().toString();
				// new_distance = distance + weight
				int new_dist = line._2._1() + (int) e.getValue();  
				//initial add any number equals to initial
				if (line._2._1() == Integer.MAX_VALUE)
					new_dist = Integer.MAX_VALUE;
				List<String> newPath = new ArrayList<String>();
				//for the first iteration this should be start node
				newPath.addAll(line._2._2().nodes); 
				//new node add to path
				newPath.add(e.getKey().toString());
				//I found it does not matter if HashMap is empty, it could match the previous data
				HashMap<String, Integer> emptyMap = new HashMap<String, Integer>(); 
				result.add(new Tuple2<>(neighborNode,                              
			               new Tuple3<>(new_dist, 
			                		    new Path(newPath),
			                		    emptyMap)));
			}
			result.add(line);		
			return result.iterator();
		});
		JavaPairRDD <String, Iterable<Tuple3<Integer, Path, HashMap<String, Integer>>>> interRDD = map1.groupByKey();
		//after iteration finding the shortest path and distance
		JavaPairRDD <String, Tuple3<Integer, Path, HashMap<String, Integer>>> reduce1 = interRDD.mapToPair(line -> {
			HashMap<String, Integer> neighborList = new HashMap<String, Integer>();
			int minDist = Integer.MAX_VALUE;
			Path shortestPath = new Path(new ArrayList<String>());
			for(Tuple3<Integer, Path, HashMap<String, Integer>> e: line._2) {
				if(!e._3().isEmpty())
					neighborList = e._3();
				// judge if its the shortest path
				if(e._1()<minDist) {
					minDist = e._1();
					List<String> l = e._2().nodes;
					shortestPath = new Path(l);
				}
			}
			return new Tuple2<>(line._1, new Tuple3<>(minDist, shortestPath, neighborList));
		});
		return reduce1;
	}
	
	// Comparator distance for sorting
	public static class CompareInt implements Comparator<Integer>, Serializable{
		@Override
		public int compare(Integer a, Integer b) {
			return a-b;
		}
	}
	
	public static class Path implements Serializable{
		public List<String> nodes;
		public Path(List<String> Nodes) {
			nodes = Nodes;
		}
		@Override
		public String toString() {
			StringBuilder stringbuilder = new StringBuilder();	
			String addString = "";
			for (String e : nodes) {
				stringbuilder.append(addString);
				addString = "-";
				stringbuilder.append(e);
			}		
			return stringbuilder.toString();
		}
	}
	
	public static void main(String[] args) {
		
	SparkConf conf = new SparkConf().setMaster("local").setAppName("Ass2");
	
	// for deleting output file if it exists
	conf.set("spark.hadoop.validateOutputSpecs", "false");
	
	// get rid of info warning
	Logger.getLogger("org").setLevel(Level.OFF);
	Logger.getLogger("akka").setLevel(Level.OFF);
	
	JavaSparkContext context = new JavaSparkContext(conf);
	String StartNode = args[0];
	JavaRDD<String> input = context.textFile(args[1]);
	JavaPairRDD<String, Tuple2<String, Integer>> transformation = input.mapToPair(line -> {
		String[] NodeAndNum = line.split(",");
		return new Tuple2<>(NodeAndNum[0], new Tuple2<>(NodeAndNum[1], Integer.valueOf(NodeAndNum[2])));
	});
	JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> order_input = transformation.groupByKey();
	JavaRDD<String> nodes = input.flatMap(line -> {
		List<String> list = new ArrayList<>();
		String[] NodeandNum = line.split(",");
		list.add(NodeandNum[0]);
		list.add(NodeandNum[1]);
		return list.iterator();
	}).distinct();
	
	int numNodes = (int) nodes.count();
	
	JavaPairRDD<String, HashMap<String, Integer>> HashMapInput = order_input.mapToPair(line ->{
		HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
		for(Tuple2<String, Integer> e :line._2) {
			hashMap.put(e._1, e._2);
		}
		return new Tuple2<>(line._1, hashMap);
	});
	
	JavaPairRDD<String, Tuple3<Integer, Path, HashMap<String, Integer>>> expect_input = 
		HashMapInput.mapToPair(line -> {		
			//initial distance to Infinity for all nodes
			Integer dist = Integer.MAX_VALUE;
			//if its the start node the distance should be 0
			if (line._1.equals(args[0])) {
				dist = 0;
			}
			//initial the path for all nodes, the path should be itself
			List<String> currNode = new ArrayList<String>();
			currNode.add(line._1);
			Path path = new Path(currNode);
			return new Tuple2<>(line._1, new Tuple3<>(dist, path, line._2));
		});
	JavaPairRDD <String, Tuple3<Integer, Path, HashMap<String, Integer>>> inter = expect_input;
	//starting iteration for nodes account - 1 times
	for (int i = 1; i < numNodes ; i++ ) {
		inter = BFS(inter);
	}
	//get rid of the start Node 
	//format the output
	JavaRDD<String> output = inter.filter(line -> !line._1.equals(StartNode)).mapToPair(line->{
		int DistForSort = line._2._1();
		//if distance equal to infinity, save it as -1
		if(DistForSort == Integer.MAX_VALUE) {
			DistForSort = -1;
		}
		return new Tuple2<>(DistForSort, new Tuple2<>(line._1, line._2._2())); 
	//sort by distance	
	}).sortByKey(new CompareInt()).map(x -> x._2._1 + "," + x._1 + "," + x._2._2);
	//output.collect().forEach(System.out::println);
	output.saveAsTextFile(args[2]);
	context.close();
	}
}

