package bigdata.andy.net.hadoopelt;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HadoopELTReducer extends Reducer<Text, Text, NullWritable, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
		// reducer is simply dump the k-v it receives
		for (Text text: values) {
			context.write(NullWritable.get(), text);
		}
		
	}

}
