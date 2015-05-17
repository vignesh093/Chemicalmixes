import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Chemicalmixdriver
{
	public static void main(String args[]) throws Exception
	{
		if(args.length !=2)
		{
			System.err.println("Usage: Worddrivernewapi <input path> <output path>");
			System.exit(-1);
		}
		Job job=new Job();
		job.setJarByClass(Chemicalmixdriver.class);
		job.setJobName("Chemicalmixdriver");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.setMapperClass(Chemicalmixmapper.class);
		job.setReducerClass(Chemicalmixreducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
	
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
