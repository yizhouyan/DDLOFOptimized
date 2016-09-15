package sampling;

import java.util.Hashtable;
import java.util.Iterator;

/**
 * PartitionPlan class contains statistics for each temp partition (partitions
 * will change)
 *
 * @author Yizhou Yan
 * @version Dec 31, 2015
 */

public class PartitionPlan {

	/** dataPoints : list of points in the partition */
	public Hashtable<String, Integer> dataPoints;

	/** show the number of buckets for each dimension */
	private int[] numBuckets_dim;

	/** number of dimensions */
	private int num_dim;

	/** number of data in this partition */
	private int num_data;

	/** start of different dimensions of domain */
	private double[] startDomain;

	/** end of different dimensions of domain */
	private double[] endDomain;

	/**
	 * number of maps: when come with a node, map to a range (divide the domain
	 * into map_num) same as DDrivenPartition2D
	 */
	private int map_num;

	/** domain/map_num = smallDomain */
	private int smallDomain;

	/**
	 * Default constructor for a PartitionPlan structure
	 */
	public PartitionPlan() {
	}

	/**
	 * get total number of data
	 * 
	 * @return num_data
	 */
	public int getNumData() {
		return num_data;
	}

	/**
	 * setup init information for Partition Plan
	 * 
	 * @param num_data
	 * @param num_dim
	 * @param numBuckets_dim
	 * @param startDomain
	 * @param endDomain
	 * @param map_num
	 * @param smallDomain
	 */
	public void setupPartitionPlan(int num_dim, int num_data, int[] numBuckets_dim, double[] startDomain,
			double[] endDomain, int map_num, int smallDomain) {
		dataPoints = new Hashtable<String, Integer>();
		this.num_dim = num_dim;
		this.num_data = num_data;
		this.numBuckets_dim = new int[numBuckets_dim.length];
		this.numBuckets_dim = numBuckets_dim;
		this.startDomain = new double[num_dim];
		this.startDomain = startDomain;
		this.endDomain = new double[num_dim];
		this.endDomain = endDomain;
		this.map_num = map_num;
		this.smallDomain = smallDomain;
	}

	/**
	 * set total data points
	 * 
	 * @param points
	 *            : total points
	 */
	public void addDataPoints(Hashtable<String, Integer> points) {
		dataPoints = points;
	}

	public boolean checkElementsInFrequencies(int[] frequencies, int index_i) {
		for (int i = index_i; i < frequencies.length; i++) {
			if (frequencies[i] != 0)
				return true;
		}
		return false;
	}

	public PartitionPlan[] seperatePartitions(int byDim, int K) {
		PartitionPlan[] newPartitions = new PartitionPlan[numBuckets_dim[byDim]];
		for (int i = 0; i < numBuckets_dim[byDim]; i++) {
			newPartitions[i] = new PartitionPlan();
		}
		int each_part_num = num_data / numBuckets_dim[byDim];
		int[] frequencies = new int[map_num];

		// load data into frequency
		for (Iterator itr = dataPoints.keySet().iterator(); itr.hasNext();) {
			String key = (String) itr.next();
			int indexOfKey = (int) Math.floor(Double.valueOf(key.split(",")[byDim]) / smallDomain);
			int value = (int) dataPoints.get(key);
			// System.out.println("Index of key : " +indexOfKey + "Small Domain:
			// "+ smallDomain );
			frequencies[indexOfKey] += value;
		}

		int index_i = 0;
		int i = 0;
		for (i = 0; i < numBuckets_dim[byDim] - 1; i++) {
			int tempsum = 0;
			double new_start_point = index_i * smallDomain;
			// System.out.println(index_i+"-----");
			boolean checkIfElements = true;
			while (tempsum < each_part_num) {
				if (!checkElementsInFrequencies(frequencies, index_i)) {
					checkIfElements = false;
					break;
				} else if ((tempsum > K) && (tempsum > 0.5 * each_part_num) && (tempsum + frequencies[index_i] > 2 * each_part_num))
					break;
				else {
					tempsum += frequencies[index_i];
					index_i++;
				}
			}
			// System.out.println(index_i+"&----");
			if ((checkIfElements == false) && (tempsum == 0)) {
				break;
			}

			// System.out.println("index_i :"+ index_i);
			double new_end_point = index_i * smallDomain;

			// System.out.println(new_start_point + " "+ new_end_point);

			// add points to new Data Points
			Hashtable<String, Integer> newDataPoints = new Hashtable<String, Integer>();
			int new_num_data = 0;
			for (Iterator itr = dataPoints.keySet().iterator(); itr.hasNext();) {
				String key = (String) itr.next();
				int value = (int) dataPoints.get(key);
				if ((Double.valueOf(key.split(",")[byDim]) >= new_start_point)
						&& (Double.valueOf(key.split(",")[byDim]) < new_end_point)) {
					newDataPoints.put(key, value);
					new_num_data += value;
				}
			}
			double[] newstartDomain = new double[num_dim];
			double[] newendDomain = new double[num_dim];
			for (int j = 0; j < num_dim; j++) {
				newstartDomain[j] = startDomain[j];
				newendDomain[j] = endDomain[j];
			}
			newstartDomain[byDim] = new_start_point;
			newendDomain[byDim] = new_end_point;

			newPartitions[i].setupPartitionPlan(num_dim, new_num_data, numBuckets_dim, newstartDomain, newendDomain,
					map_num, smallDomain);
			newPartitions[i].addDataPoints(newDataPoints);
			// newPartitions[i].printPartitionPlan();
		} // for(int i = 0; i < numBuckets_dim[byDim]-1 ; i++)

		// setup the final partition which startpoint = index_i and endpoint =
		// org_endpoint

		double[] newstartDomain = new double[num_dim];
		double[] newendDomain = new double[num_dim];
		for (int j = 0; j < num_dim; j++) {
			newstartDomain[j] = startDomain[j];
			newendDomain[j] = endDomain[j];
		}
		double new_start_point = index_i * smallDomain;
		// System.out.println(new_start_point+"________");
		newstartDomain[byDim] = new_start_point;
		
		// add points to new Data Points
		Hashtable<String, Integer> newDataPoints = new Hashtable<String, Integer>();
		int new_num_data = 0;
	
		for (Iterator itr = dataPoints.keySet().iterator(); itr.hasNext();) {
			String key = (String) itr.next();
			int value = (int) dataPoints.get(key);
			if ((Double.valueOf(key.split(",")[byDim]) >= new_start_point)
					&& (Double.valueOf(key.split(",")[byDim]) < endDomain[byDim])) {
				newDataPoints.put(key, value);
				new_num_data += value;
			}
		}
	//	newPartitions[i-1].printPartitionPlan();
		if (new_num_data < K && i >= 1) {
			// if final partition has less than K data points, merge with that
			// partition
			int final_new_num_data = new_num_data + newPartitions[i - 1].num_data;
	//		System.err.println("final new num data: "+ final_new_num_data);
			newstartDomain[byDim] = newPartitions[i - 1].startDomain[byDim];
			newDataPoints.putAll(newPartitions[i-1].dataPoints);
			newPartitions[i - 1].setupPartitionPlan(num_dim, final_new_num_data, numBuckets_dim, newstartDomain,
					newendDomain, map_num, smallDomain);
	//		System.out.println("Added " + newDataPoints.size());
//			for (Iterator itr = newPartitions[i - 1].dataPoints.keySet().iterator(); itr.hasNext();) {
//				String key = (String) itr.next();
//				int value = (int) newPartitions[i - 1].dataPoints.get(key);
//				newDataPoints.put(key, value);
//			}
			newPartitions[i - 1].addDataPoints(newDataPoints);
	//		newPartitions[i-1].printPartitionPlan();
		} else {
			newPartitions[i].setupPartitionPlan(num_dim, new_num_data, numBuckets_dim, newstartDomain, newendDomain,
					map_num, smallDomain);
			newPartitions[i].addDataPoints(newDataPoints);
	//		newPartitions[i].printPartitionPlan();
		}
		// newPartitions[i].printPartitionPlan();

		return newPartitions;
	}

	public String getStartAndEnd() {
		String startEnd = "";
		for (int i = 0; i < num_dim; i++) {
			startEnd = startEnd + startDomain[i] + "," + endDomain[i] + ",";
		}
		if (startEnd.length() >= 1)
			startEnd = startEnd.substring(0, startEnd.length() - 1);
		return startEnd;
	}

	public void printPartitionPlan() {
		System.out.println("number of points in this partition:" + this.num_data);
		System.out.println("data points list: ");
		for (Iterator itr = dataPoints.keySet().iterator(); itr.hasNext();) {
			String key = (String) itr.next();
			Integer value = (Integer) dataPoints.get(key);
			System.out.println(key + "--" + value);
		}
		System.out.println("number of dimensions" + this.num_dim);
		System.out.println("Number of desired buckets per dimension: ");
		for (int i = 0; i < numBuckets_dim.length; i++) {
			System.out.print(numBuckets_dim[i] + "        ");
		}
		System.out.println("");

		System.out.println("Start points and end points: ");
		for (int i = 0; i < num_dim; i++) {
			System.out.println("Start from " + startDomain[i] + "   end to " + endDomain[i]);
		}
		System.out.println("----------------------------------------------------------------------------");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
