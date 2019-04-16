package kmean.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeanIterationMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] dimensions = value.toString().split(",") ;
		
		Configuration conf = (Configuration) context.getConfiguration() ;
		int k = Integer.valueOf(conf.get("kmeans.k")) ;
		
		double min_distance = Double.MAX_VALUE ;
		int best_centroid_index = -1 ;
		
		for ( int i = 0 ; i < k ; i ++) {
			
			String centroid = conf.get("kmeans.centroid" + Integer.toString(i)) ;
			String[] centroid_dim_split = centroid.split(",") ;
			
			double distance = 0 ;
			for(int j = 0 ; j < dimensions.length-1  ; j++) {
				double point_dim = Double.parseDouble(dimensions[j]) ;
				double centroid_dim = Double.parseDouble(centroid_dim_split[j]) ;
				distance += Math.pow(point_dim - centroid_dim, 2) ; 
			}
			
			if(distance < min_distance) {
				min_distance = distance ; 
				best_centroid_index = i ;
			}
		}
		context.write(new IntWritable(best_centroid_index) , value);
	}
	
}
