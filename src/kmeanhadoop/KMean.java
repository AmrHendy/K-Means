package kmeanhadoop;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMean {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		if(args.length != 4) {
			System.out.print("args should be 2 : <inputpath> <outpath> <number of centroids> <dimensions> .") ;
			System.exit(-1);
		}
		
		Job init_job = Job.getInstance() ;
		
		Configuration init_conf = init_job.getConfiguration();
		init_conf.set("Kmeans.k", args[2]);
		init_conf.set("Kmeans.dim", args[3]);
		
		init_job.setJarByClass(KMean.class);
		init_job.setJobName("clustered kmeans");
		init_job.setMapperClass(KMeanInitMapper.class);
		init_job.setReducerClass(KMeanInitReducer.class);
		init_job.setOutputKeyClass(IntWritable.class);
		init_job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(init_job, new Path(args[0]));
		//TODO check rewrite in future
		FileOutputFormat.setOutputPath(init_job, new Path(args[1]));
		
		init_job.waitForCompletion(true);
		
		
		while(true) {
			
			Job kmean_cluster_jb = Job.getInstance() ;
			Configuration conf = kmean_cluster_jb.getConfiguration() ;
			conf.set("Kmeans.k", args[2]);
			conf.set("Kmeans.dim", args[3]);
			
			kmean_cluster_jb.setJarByClass(KMean.class);
			kmean_cluster_jb.setJobName("clustered kmeans");
			kmean_cluster_jb.setMapperClass(KMeanIterationMapper.class);
			kmean_cluster_jb.setReducerClass(KMeanIterationReducer.class);
			kmean_cluster_jb.setOutputKeyClass(IntWritable.class);
			kmean_cluster_jb.setOutputValueClass(Text.class);
			
			//TODO check rewrite in future
			String uri =  args[1];
			Configuration temp_conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(uri), temp_conf); 
			Path input_path = new Path(uri);
			FSDataInputStream input_stream = fs.open(input_path);
			
			int k = Integer.valueOf(args[2]);
			for(int i = 0 ; i < k ; i++) {	
				String line = input_stream.readL ; 
			}
			
			
			
		}
		
		
		
		
		
		
	}
	
}
