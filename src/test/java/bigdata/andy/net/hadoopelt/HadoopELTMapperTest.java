package bigdata.andy.net.hadoopelt;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

public class HadoopELTMapperTest {

	@Test
	public void testMapLongWritableTextContext() throws IOException {
		
		
		new MapDriver<LongWritable, Text, Text, Text>()
		.withMapper(new HadoopELTMapper())
		.withCacheFile("sm.state")
		.withCacheFile("sm.supersector")
		.withInput(new LongWritable(12), new Text("SMS01000000000000001          	1990	M01	      1626.7"))
		.withOutput(new Text("Alabama"), new Text("Alabama,Total Nonfarm,1990,M01,1626.7"))
		.runTest();
	}

}
