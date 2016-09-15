package sampling;

/** this file aims to calculate chi square for each dimension */

public class Cal_chisquare {

	/**
	 * Calculates the chi-square value for N positive integers less than r
	 * Source: "Algorithms in C" - Robert Sedgewick - pp. 517
	 * @param  randomNums  an array of integers
	 * @param  r           upper bound for the range
	 * @return             chi square value for the array
	 * @author Yizhou Yan
	 * @version Sep 28, 2015
	 */
	
	public static double Chi_square(int[] randomNums, int r)
	{
		double n_r = (double)randomNums.length / r*1.0;
		double chiSquare = 0;

		for(int i = 0; i < randomNums.length; i++){
			double f = randomNums[i] - n_r;
			chiSquare += f * f;
		}
		
		chiSquare /= n_r;
		return chiSquare;
	}
	
	/**
	 * chi-square value are calculated for Integers, the function is to change double array to integer array
	 * @param  data  an array of doubles
	 * @param  scale         the mini-scale of the array
	 * @return          an array of integers
	 * @author Yizhou Yan
	 * @version Sep 28, 2015
	 */
	
	public static int[] changeDoubleToInt(double[] data, double scale){
		int [] intdata = new int[data.length];
		
		for(int i=0;i<data.length;i++){
			intdata[i] = (int) (data[i]/scale);
			System.out.println(intdata[i]);
		}
		return intdata;
	}
	
	/**
	 * Call chi square function and manage double-int
	 * @param  data  an array of doubles
	 * @param  data_unit         the mini-scale of the array
	 * @return     chi square value
	 * @author Yizhou Yan
	 * @version Sep 28, 2015
	 */
	
	public static double Cal_Chisquare(double [] data, double data_unit){
		int []intdata = changeDoubleToInt(data,data_unit);
		int max_data = 0;
		for(int i = 0; i< intdata.length; i++){
			if(intdata[i]>max_data)
				max_data = intdata[i];
		}
		return Chi_square(intdata,max_data);
	}
	
	/**unit test of Cal_Chisquare*/
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		double []data = {0.1,0.1,0.4};
		System.out.println(Cal_Chisquare(data,0.1));
	}

}
