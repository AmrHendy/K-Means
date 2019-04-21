import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
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
		this.sc = new JavaSparkContext(conf);
	}
	
	public void run() {
		JavaRDD<String> data  = this.sc.textFile(this.inputPath);
		JavaRDD<Vector> all_points = data.map(line -> {
			String[] sarray = line.split(" ");
			double[] values = new double[sarray.length];
			for (int i = 0; i < sarray.length; i++) {
				values[i] = Double.parseDouble(sarray[i]);
			}
			return Vectors.dense(values);		
		}) ;
		
		ArrayList<Vector> centroids = (ArrayList<Vector>) all_points.take(numCentroids) ;
		ArrayList<Integer> setCounts = new ArrayList<Integer>(centroids.size()) ;
		
		JavaRDD<Vector> rdd_centroids = sc.parallelize(centroids) ;
		JavaRDD<Integer> rdd_setCounts = sc.parallelize(setCounts) ;
		
		while(true) {
			
			JavaPairRDD<Integer,Vector> points = all_points.mapToPair(point ->{
				int centroidAssigned = -1 ; 
				double minDistance = Integer.MAX_VALUE ;
				for(int i = 0 ; i < rdd_centroids.collect().size() ; i++) {
					double currentDistance = distance(rdd_centroids.collect().get(centroidAssigned), point);  
					if(currentDistance < minDistance) {
						minDistance = currentDistance ;
						centroidAssigned = i ;
					}
				}
				rdd_setCounts.collect().set(centroidAssigned - 1, rdd_setCounts.collect().get(centroidAssigned) + 1);
				return new Tuple2<Integer,Vector>(centroidAssigned, point);
			});
			
			points = points.cache();
			
			JavaPairRDD<Integer,Vector> calculated_centroids = points.reduceByKey(new Function2<Vector, Vector, Vector>() {
				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;
				
				@Override
				public Vector call(Vector point1, Vector point2) throws Exception {
					// TODO Auto-generated method stub
					double[] total = new double[point1.size()];
					for(int i = 0 ; i < point1.size() ; i++) {
						total[i] = point1.apply(i) + point2.apply(i);
					}
					return Vectors.dense(total);
				}
			}).sortByKey();
			
			ArrayList<Vector> old_centroids = centroids ;
			
			calculated_centroids.mapToPair( element ->{
				return new Tuple2<Integer,Vector>(element._1, average(element._2, rdd_setCounts.collect().get(element._1))) ;
			});
			
			centroids = (ArrayList<Vector>) calculated_centroids.values().collect();
			for(int i = 0 ; i < centroids.size() ; i++) {
				centroids.set(i, average(centroids.get(i), setCounts.get(i))) ;
			}
			setCounts = new ArrayList<Integer>(centroids.size()) ;
			
			double total_diff = 0 ;
			for(int i = 0 ; i < old_centroids.size() ; i++) {
				total_diff += difference(old_centroids.get(i), centroids.get(i)) ;
			}
			
			double threshold = Math.pow(0.001 ,2) * this.numCentroids * this.dimensions  ;
			if(total_diff < threshold) {
				calculated_centroids.values().saveAsTextFile(outPath);
				break ;
			}
		}		
		this.sc.close();

	}

	private Vector average(Vector v1, int n) {
		double[] averaged = v1.toArray() ;
		for(int i = 0 ; i < v1.size() ; i++) {
			averaged[i] = averaged[i] / n;
		}
		return Vectors.dense(averaged) ;
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
