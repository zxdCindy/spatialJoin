package spatialJoin;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generate 10M input data files
 * @author cindyzhang
 *
 */
public class GenerateValues {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		ReadWriteFile myReadWrite = new ReadWriteFile();
		String fileName = "/Users/cindyzhang/Desktop/generate/bar.txt";
		File file =new File(fileName);
		List<String> values = new ArrayList<String>();
		DecimalFormat df = new DecimalFormat("#0.00000");

		do{
			values.clear();
			for(int i=0; i < 10000; i++){
				values.add(df.format(randomValue(10000))+","
						+df.format(randomValue(10000)));
				//values.add(df.format(randomValue(1000000000))+","
						//+df.format(randomValue(1000000000)));
			}
			myReadWrite.writeLargeTextFile(fileName, values);
			System.out.println(checkSize(file));
		}while(checkSize(file) < 10);

	}
	
	/**
	 * Generate random values between -range and range
	 * @param range
	 * @return
	 */
	public static double randomValue(double range){		
		Random generator = new Random();
		double value = generator.nextDouble() * (range+range);
		value = value - range;		
		return value;
	}
	
	/**
	 * check the size of a file
	 * @param file
	 * @return
	 */
	public static double checkSize(File file){
		double bytes = file.length();
		double kilobytes = (bytes / 1024);
		double megabytes = (kilobytes / 1024);
//		double gigabytes = (megabytes / 1024);
//		double terabytes = (gigabytes / 1024);
//		double petabytes = (terabytes / 1024);
//		double exabytes = (petabytes / 1024);
//		double zettabytes = (exabytes / 1024);
//		double yottabytes = (zettabytes / 1024);
		return megabytes;
	}

}
