package bigdata.andy.net.hadoopelt;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class HadoopELTTest {

	@Test
	public void testRun() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "file:///");
		conf.set("mapreduce.framework.name", "local");
		conf.setInt("mapreduce.task.io.sort.mb", 1);
		
		Path input = new Path("test.data");
		Path output = new Path("/tmp/test");
		
		FileSystem fs = FileSystem.getLocal(conf);
		fs.delete(output, true);
		
		HadoopELT driver = new HadoopELT();
		driver.setConf(conf);
		
		int exitCode = driver.run(new String[] {input.toString(), output.toString()});
		assertThat(exitCode, is(0));
		
	}

}
