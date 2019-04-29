public class SparkKmean {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		/*
			"./bin/spark-submit \
			  --class <main-class> \
			  --master <master-url> \ local[K]	Run Spark locally with K worker threads (ideally, set this to the number of cores on your machine)
			  --deploy-mode <deploy-mode> \
			  --conf <key>=<value> \
			  ... # other options
			  <application-jar> \
			  [application-arguments]"
		*/
		
		if(args.length != 4) {
			System.out.print("args should be 2 : <inputpath> <outpath> <number of centroids> <dimensions> .") ;
			System.exit(-1);
		}
		
		String inputPath = args[0] ;
		String outPath = args[1] ;
		Integer numCentroids = Integer.valueOf(args[2]) ;
		Integer dimensions = Integer.valueOf(args[3]) ;
		KmeansImpl impl = new KmeansImpl(inputPath, outPath, numCentroids, dimensions) ;

		impl.run();
	}

}
