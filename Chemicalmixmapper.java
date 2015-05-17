import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Chemicalmixmapper extends Mapper<LongWritable,Text,Text,DoubleWritable>
{
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
	{
		String value1=value.toString();
		String[] wholeval=value1.split(";");
		int[] columnno={0,2,3,4,5,6,7};
		String[] columnvalues={"fixed acidity","citric acid","residual sugar","chlorides","free sulfur dioxide","total sulfur dioxide","density"};

		for(int i=0;i<7;i++)
		{
			float k=Float.parseFloat( wholeval[columnno[i]]);
		context.write(new Text(columnvalues[i]), new DoubleWritable(k));
		}
	}

}
