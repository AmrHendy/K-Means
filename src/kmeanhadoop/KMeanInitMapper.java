package kmeanhadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeanInitMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String[] arr =  value.toString().split(",") ;
		StringBuilder sb = new StringBuilder() ;
		for(int i = 0 ; i < arr.length -1 ; i++) {
			sb.append(arr[i]);
			if(i != arr.length -2) {
				sb.append(",");
			}
		}
		context.write(new IntWritable(1), new Text(sb.toString()));			
	}

	@Override
	public void run(Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration() ;
		int k = Integer.valueOf(conf.get("kmeans.k")) ;
		
		// TODO this is weird 
		setup(context) ;
		
		for( int i = 0 ; i < k ; i ++) {
			if(!context.nextKeyValue())
				break ;
			map(context.getCurrentKey() , context.getCurrentValue() , context) ;
		}
	}
	
}
