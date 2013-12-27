package spatialJoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Find all the foos which are less than 1 unit than a bar
 * @author cindyzhang
 *
 */
public class FooBar {

	/**
	 * Generate the 9 grids which the bar point belongs to
	 *
	 */
	public static class BarMapper 
		extends Mapper<LongWritable, Text, PointTuple, PointTuple>{
		PointTuple outputKey = new PointTuple();
		PointTuple outputValue = new PointTuple();
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			if(value.toString() != null){
				String[] points = value.toString().split(",");
				double x = Double.valueOf(points[0]);
				double y = Double.valueOf(points[1]);
				outputValue.setX(x);
				outputValue.setY(y);
				outputValue.setSet("bar");
				
				//9 grids
				double ceilX = x < 0 ? Math.floor(x) : Math.ceil(x);
				double ceilY = y < 0 ? Math.floor(y) : Math.ceil(y);
				for(int i=-1;i<=1;i++){
					for(int j=-1;j<=1;j++){
						outputKey.setX(ceilX+i);
						outputKey.setY(ceilY+j);	
						context.write(outputKey, outputValue);
					}
				}
			}
		}
	}
	
	/**
	 * Generate the grid where the foo point belongs to
	 *
	 */
	public static class FooMapper 
		extends Mapper<LongWritable, Text, PointTuple, PointTuple>{
		PointTuple outputKey = new PointTuple();
		PointTuple outputValue = new PointTuple();
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			if(value.toString() != null){
				String[] points = value.toString().split(",");
				double x = Double.valueOf(points[0]);
				double y = Double.valueOf(points[1]);
				outputValue.setX(x);
				outputValue.setY(y);
				outputValue.setSet("foo");
				
				double ceilX = x < 0 ? Math.floor(x) : Math.ceil(x);
				double ceilY = y < 0 ? Math.floor(y) : Math.ceil(y);
				outputKey.setX(ceilX);
				outputKey.setY(ceilY);
				
				context.write(outputKey, outputValue);
			}
		}
	}
	
	public static class FooBarReducer 
		extends Reducer<PointTuple, PointTuple, PointTuple, PointTuple>{

		public void reduce(PointTuple key, Iterable<PointTuple> values, Context context) 
				throws IOException, InterruptedException{
			List<PointTuple> pointList = new ArrayList<PointTuple>();
//			System.out.println(key);
			for(PointTuple p : values){
				pointList.add((PointTuple)p.clone());
			}

			//Check the distance, if it's less than 1, print it out
			for(PointTuple p1: pointList){
				for(PointTuple p2: pointList){
					if(p1.getSet().equals("bar") && p2.getSet().equals("foo")){
						double powValue = Math.pow((p1.getX()-p2.getX()), 2.0)
								+ Math.pow((p1.getY()-p2.getY()), 2.0);
						double distance = Math.sqrt(powValue);
						if(distance < 1.0){
							context.write(p1, p2);
						}
					}
				}
			}
		}
	}
	
	/**
	 * @param args
	 * @throws InterruptedException 
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "FooBarJob");
		
		Path fooInput = new Path(args[0]);
		Path barInput = new Path(args[1]);
		Path outputDir = new Path(args[2]);
		
		job.setJarByClass(FooBar.class);
		job.setOutputKeyClass(PointTuple.class);
		job.setOutputValueClass(PointTuple.class);
		MultipleInputs.addInputPath(job, fooInput, 
				TextInputFormat.class,FooMapper.class);
		MultipleInputs.addInputPath(job, barInput, 
				TextInputFormat.class,BarMapper.class);
		job.setReducerClass(FooBarReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, outputDir);		
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}

}
