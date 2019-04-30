import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;

import scala.Tuple2;

public class KmeansImpl {
	
	private String inputPath;
	private String outPath;
	private Integer numCentroids;
	private Integer dimensions;
	private JavaSparkContext sc;
	
	public KmeansImpl(String inputPath, String outPath, Integer numCentroids, Integer dimensions) {
		super();
		this.inputPath = inputPath;
		this.outPath = outPath;
		this.numCentroids = numCentroids;
		this.dimensions = dimensions;
		String appName = "kmeans";
		SparkConf conf = new SparkConf().setAppName(appName);
		conf.setMaster("local[4]");
		this.sc = new JavaSparkContext(conf);
		this.sc.setLogLevel("WARN");
	}
	
	public void run() {
		JavaRDD<String> data  = this.sc.textFile(this.inputPath);
		JavaRDD<Vector> all_points = data.map(line -> {
			String[] sarray = line.split(",");
			double[] values = new double[sarray.length - 1];
			for (int i = 0; i < sarray.length - 1 ; i++) {
				values[i] = Double.parseDouble(sarray[i]);
			}
			return Vectors.dense(values);		
		}) ;
		
		System.out.println("number of centroids are : "  + numCentroids);
		
		java.util.List<Vector> centroids =  all_points.take(numCentroids) ;
		
		ArrayList<Tuple2<Integer, Vector>> centroids_p = new ArrayList<Tuple2<Integer,Vector>>();
		
		for(int i = 0 ; i < centroids.size() ; i++) {
			centroids_p.add(new Tuple2<Integer, Vector>(i, centroids.get(i))) ;
		}
	
		JavaPairRDD<Integer,Vector> old_centroids = sc.parallelizePairs(centroids_p) ;
		
		old_centroids.foreach(point->{
			System.out.println("centroid " + point._1 + " => " + point);
		});
		
		while(true) {
			
			old_centroids.foreach(point->{
				System.out.println("old centroid" + point._1 + " => " + point);
			});

			
			java.util.List<Vector> lis = old_centroids.sortByKey().values().collect() ;

			JavaPairRDD<Integer,Vector> points = all_points.mapToPair(point ->{
				int centroidAssigned = -1 ; 
				double minDistance = Integer.MAX_VALUE ;
				for(int i = 0 ; i < lis.size() ; i++) {
					double totalSquare = 0 ;
					Vector v1 = lis.get(i);
					Vector v2 = point;
					for(int j = 0 ; j < v1.size() ; j++) {
						totalSquare += Math.pow(v1.apply(j) - v2.apply(j), 2);
					}
					double currentDistance = Math.sqrt(totalSquare) ;  
					if(currentDistance < minDistance) {
						minDistance = currentDistance ;
						centroidAssigned = i ;
					}
				}
				return new Tuple2<Integer,Vector>(centroidAssigned, point);
			});

			points = points.cache();

			JavaPairRDD<Integer,Vector> calculated_centroids = points.reduceByKey((point,sum)->{
				double[] total = new double[point.size()];
				for(int i = 0 ; i < point.size() ; i++) {
					total[i] = point.apply(i) + sum.apply(i);
				}
				return Vectors.dense(total);
			});

			calculated_centroids.foreach(point->{
				System.out.println("calculated centroid sum" + point._1 + " => " + point);
			});
			
			JavaPairRDD<Integer,Integer> counts = points.mapToPair(t -> new Tuple2<>(t._1, 1))
					.reduceByKey((a, b) -> a + b);
			
			counts.foreach(point->{
				System.out.println("calculated centroid count" + point._1 + " => " + point);
			});
						
			JavaPairRDD<Integer,Vector> new_centroids = counts.join(calculated_centroids).mapToPair(res->{
				double[] total = new double[res._2._2.size()];
				for(int i = 0 ; i < res._2._2.size() ; i++) {
					total[i] = res._2._2.apply(i) / res._2._1.doubleValue() ;
				}
				return new Tuple2<Integer, Vector>(res._1, Vectors.dense(total));
			});
			
			new_centroids.foreach(point->{
				System.out.println("new centroid " + point._1 + " => " + point);
			});
			
			JavaPairRDD<Integer, Double> diff = old_centroids.join(new_centroids).mapToPair(res -> {
				double sum = 0 ;
				for(int i = 0 ; i < res._2._1.size() ; i++) {
					sum += Math.pow(res._2._1.apply(i) - res._2._2.apply(i) , 2) ;
				}
				return new Tuple2<Integer, Double>(1, sum);
			}).reduceByKey((a, b) -> a + b);
			
			diff.foreach(point->{
				System.out.println("new diff" + point._1 + " => " + point);
			});
			
			double threshold = Math.pow(0.001 ,2) * this.numCentroids * this.dimensions  ;
			if(diff.values().collect().get(0) < threshold) {
				new_centroids.saveAsTextFile(outPath);
				break ;
			}	
			
			old_centroids = new_centroids ; 
			
		}		
		this.sc.close();

	}

	public double difference(Vector v1, Vector v2) {
		double total = 0 ;
		for(int i = 0 ; i < v1.size() ; i++) {
			total += Math.abs(v1.apply(i) - v2.apply(i));
		}
		return total ;
	}
	
	public double distance(Vector v1, Vector v2) {
		double totalSquare = 0 ;
		for(int i = 0 ; i < v1.size() ; i++) {
			totalSquare += Math.pow(v1.apply(i) - v2.apply(i), 2);
		}
		return Math.sqrt(totalSquare) ;
	}
}
