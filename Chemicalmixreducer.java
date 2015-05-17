import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Chemicalmixreducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
	HashMap<String,Double> mymap=new HashMap<String,Double>();
	public void reduce(Text key,Iterable<DoubleWritable> value,Context write) throws IOException,InterruptedException
	{
		
		double k=0;
		for(DoubleWritable values: value)
		{
			k+=values.get();
		}
		mymap.put(key.toString(), k);
	}

	
	public void run(Context context) throws IOException, InterruptedException 
	{
		setup(context);
		while(context.nextKeyValue())
		{
			reduce(context.getCurrentKey(), context.getValues(), context);
		}
		
		double favalue=mymap.get("fixed acidity");
		double cavalue=mymap.get("citric acid");
		double rsvalue=mymap.get("residual sugar");
		double chvalue=mymap.get("chlorides");
		double fsdvalue=mymap.get("free sulfur dioxide");
		double tsvalue=mymap.get("total sulfur dioxide");
		double dnvalue=mymap.get("density");
		
		double averca=(cavalue/favalue)*100;
		double avera=(rsvalue/favalue)*100;
		double avech=(chvalue/favalue)*100;
		double avefsd=(fsdvalue/favalue)*100;
		double avets=(tsvalue/favalue)*100;
		double avedn=(dnvalue/favalue)*100;
	
	context.write(new Text("citric acid"),new DoubleWritable(averca));
	context.write(new Text("residual sugar"),new DoubleWritable(avera));
	context.write(new Text("chlorides"),new DoubleWritable(avech));
	context.write(new Text("free sulfur dioxide"),new DoubleWritable(avefsd));
	context.write(new Text("total sulfur dioxide"),new DoubleWritable(avets));
	context.write(new Text("density"),new DoubleWritable(avedn));
	}
	
}
