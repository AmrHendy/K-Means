package kmean.sequential;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class KMeansSequential {

	public static void main(String[] args) throws NumberFormatException, IOException {
		
		if(args.length != 4) {
			System.out.print("args should be 2 : <inputpath> <outpath> <number of centroids> <dimensions> .") ;
			System.exit(-1);
		}
		
		int k = Integer.valueOf(args[2]) ;
		int dim = Integer.valueOf(args[3]) ;
		
		// reading the iris dataset
		List<ArrayList<Double>> all_pts = new ArrayList<ArrayList<Double>>();
		String input_file_path = args[0];
		BufferedReader br = new BufferedReader(new FileReader(new File(input_file_path))); 
		String line; 
		while ((line = br.readLine()) != null) {
			ArrayList<Double> point = new ArrayList<Double>();
			String[] dims = line.split(",");
			for(int i = 0 ; i < dims.length -1 ; i++){
				point.add(Double.parseDouble(dims[i]));
			}
			all_pts.add(point);
		}
		br.close();
		// initialize the centroids
		Double[][] centroids = new Double[k][dim];
		for(int centroid_ind = 0; centroid_ind < k; centroid_ind++){
			for(int dim_ind = 0; dim_ind < dim; dim_ind++){	
				centroids[centroid_ind][dim_ind] = all_pts.get(centroid_ind).get(dim_ind);
			}
		}

		long t1 =  System.currentTimeMillis() ;
		
		// KMeans iterations
		while(true){
			// points assignment to centroids
			List<Integer> points_assignment = new ArrayList<Integer>() ;
			HashMap<Integer, ArrayList<Integer>> centroids_groups = new HashMap<Integer, ArrayList<Integer>>();
			
			for(int point_ind = 0; point_ind < all_pts.size(); point_ind++){
				double min_distance = Double.MAX_VALUE;
				int nearest_centroid_ind = -1 ;
				for(int centroid_ind = 0; centroid_ind < k; centroid_ind++){
					double dist = 0;
					for(int dim_index = 0; dim_index < dim; dim_index++){
						//System.out.println("point_ind:" + point_ind + " centroid_ind;" + centroid_ind + " dim_index:" + dim_index) ;
						double point_dim = all_pts.get(point_ind).get(dim_index);
						double centroid_dim = centroids[centroid_ind][dim_index];
						dist += Math.pow(point_dim - centroid_dim, 2); 
					}
					if(dist < min_distance){
						min_distance = dist;
						nearest_centroid_ind = centroid_ind;
					}
				}

				points_assignment.add(nearest_centroid_ind);

				if(!centroids_groups.containsKey(nearest_centroid_ind)){
					centroids_groups.put(nearest_centroid_ind, new ArrayList<Integer>());
				}
				ArrayList<Integer> centroid_group = centroids_groups.get(nearest_centroid_ind);
				centroid_group.add(point_ind);
			}


			// update centroids
			Double[][] new_centroids = new Double[k][dim] ;
			for(int centroid_ind = 0; centroid_ind < k; centroid_ind++){
				ArrayList<Integer> centroid_group = centroids_groups.get(centroid_ind);
				if(!centroid_group.isEmpty()){
					double[] dims = new double[dim];
					for(Integer point_ind : centroid_group){
						for(int dim_ind = 0; dim_ind < dim; dim_ind++){
							dims[dim_ind] += all_pts.get(point_ind).get(dim_ind);
						}
					}
					for(int dim_ind = 0; dim_ind < dim; dim_ind++){
						new_centroids[centroid_ind][dim_ind] = dims[dim_ind] / centroid_group.size();
					}
				}
				else{
					for(int dim_ind = 0; dim_ind < dim; dim_ind++){
						new_centroids[centroid_ind][dim_ind] = centroids[centroid_ind][dim_ind];
					}
				}
			}

			// check stopping criteria
			double total_diff = 0;
			for(int centroid_ind = 0 ; centroid_ind < k ; centroid_ind++) {	
				for(int dim_ind = 0 ; dim_ind < dim ; dim_ind++) {
					total_diff += Math.pow(new_centroids[centroid_ind][dim_ind] - centroids[centroid_ind][dim_ind], 2) ;
					centroids[centroid_ind][dim_ind] = new_centroids[centroid_ind][dim_ind] ;
				}
			}
			
			double threshold = Math.pow(0.001 ,2) * k * dim  ;
			if(total_diff < threshold)
				break;
					
			System.out.println("****************************************");
			for(int i = 0 ; i < centroids.length ; i++) {
				for(int j = 0 ; j < centroids[0].length ; j++) {
					System.out.print(centroids[i][j] + " ");
				}
				System.out.println("");
			}
		}	
		
		long t2 =  System.currentTimeMillis() ;
		System.out.println("Time token by un-parallel is : " + (t2-t1) + "ms");
		
	}
}
