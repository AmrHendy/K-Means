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
	}
	
	public void run() {
		JavaRDD<String> data  = this.sc.textFile(this.inputPath);
		JavaRDD<Vector> all_points = data.map(line -> {
			String[] sarray = line.split(",");
			double[] values = new double[sarray.length];
			for (int i = 0; i < sarray.length - 1 ; i++) {
				values[i] = Double.parseDouble(sarray[i]);
			}
			return Vectors.dense(values);		
		}) ;
		
		java.util.List<Vector> centroids =  all_points.take(numCentroids) ;
		
		ArrayList<Tuple2<Integer, Vector>> centroids_p = new ArrayList<Tuple2<Integer,Vector>>();
		
		for(int i = 0 ; i < centroids.size() ; i++) {
			centroids_p.add(new Tuple2<Integer, Vector>(i, centroids.get(i))) ;
		}
	
		JavaPairRDD<Integer,Vector> old_centroids = sc.parallelizePairs(centroids_p) ;
		
		while(true) {
			
			java.util.List<Vector> lis = old_centroids.values().collect() ;
			
			JavaPairRDD<Integer,Vector> points = all_points.mapToPair(point ->{
				int centroidAssigned = -1 ; 
				double minDistance = Integer.MAX_VALUE ;
				for(int i = 0 ; i < lis.size() ; i++) {
					double currentDistance = distance(lis.get(centroidAssigned), point);  
					if(currentDistance < minDistance) {
						minDistance = currentDistance ;
						centroidAssigned = i ;
					}
				}
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
			
			
			JavaPairRDD<Integer,Integer> counts = calculated_centroids.mapToPair(t -> new Tuple2<>(t._1(), 1))
					.reduceByKey((a, b) -> a + b);
			
			JavaPairRDD<Integer,Vector> new_centroids = counts.join(calculated_centroids)
					.mapToPair(new PairFunction<Tuple2<Integer,Tuple2<Integer,Vector>>, Integer, Vector>() {
						/**
						 * 
						 */
						private static final long serialVersionUID = 1L;

						@Override								
						public Tuple2<Integer, Vector> call(Tuple2<Integer, Tuple2<Integer, Vector>> res) throws Exception {
							// TODO Auto-generated method stub
							double[] total = new double[res._2._2.size()];
							for(int i = 0 ; i < res._2._2.size() ; i++) {
								total[i] = res._2._2.apply(i) / res._2._1.doubleValue() ;
							}
							return new Tuple2<Integer, Vector>(res._1, Vectors.dense(total));
						}
					});
			
			
			JavaPairRDD<Integer, Double> diff = old_centroids.join(new_centroids).mapToPair(new PairFunction<Tuple2<Integer,Tuple2<Vector,Vector>>, Integer, Double>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<Integer, Double> call(Tuple2<Integer, Tuple2<Vector, Vector>> res) throws Exception {
					// TODO Auto-generated method stub
					double sum = 0 ;
					for(int i = 0 ; i < res._2._1.size() ; i++) {
						sum += Math.pow(res._2._1.apply(i) - res._2._2.apply(i) , 2) ;
					}
					sum = Math.sqrt(sum) ;
					
					return new Tuple2<Integer, Double>(1, sum);
				}
			}).reduceByKey((a, b) -> a + b);
			
			double threshold = Math.pow(0.001 ,2) * this.numCentroids * this.dimensions  ;
			if(diff.values().collect().get(0) < threshold) {
				new_centroids.values().saveAsTextFile(outPath);
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
