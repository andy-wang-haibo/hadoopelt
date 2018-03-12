package bigdata.andy.net.hadoopelt;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

public class HadoopELTReducerTest {

	@Test
	public void testReduceTextIterableOfTextContext() throws IOException {
		new ReduceDriver<Text, Text, NullWritable, Text>()
			.withReducer(new HadoopELTReducer())
			.withInput(new Text("Alabama"), Arrays.asList(new Text("Alabama,Total Nonfarm,1990,M01,1626.7")))
			.withOutput(NullWritable.get(), new Text("Alabama,Total Nonfarm,1990,M01,1626.7"))
			.runTest();
	}

}
