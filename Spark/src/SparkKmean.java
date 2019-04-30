public class SparkKmean {

	public static void main(String[] args) {
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
