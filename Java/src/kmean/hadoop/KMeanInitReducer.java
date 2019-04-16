package kmean.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeanInitReducer extends Reducer<IntWritable, Text, IntWritable, Text>{

	@Override
	public void reduce(IntWritable key, Iterable<Text> centroids,Context context) throws IOException, InterruptedException {
		int i = 0; 
		for(Text centroid : centroids) {
			context.write(new IntWritable(i), centroid);
			i++ ;
		}
	}
}
