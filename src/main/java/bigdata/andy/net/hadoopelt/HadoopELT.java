package bigdata.andy.net.hadoopelt;



import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HadoopELT extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		if ( args.length != 2 ) {
			System.err.format("Usage: %s [generic options] <input> <output>\n", HadoopELT.class.getSigners());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		/*
		 * parse the input file to extract employment data, and translate various 'code' into meaningful state/industry names
		 */
		Job job = Job.getInstance(getConf(), "hadoop-elt");
		job.setJarByClass(HadoopELT.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Files.deleteIfExists(Paths.get(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(HadoopELTMapper.class);
		job.setReducerClass(HadoopELTReducer.class);
		job.setNumReduceTasks(3);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true)? 0: 1;
	}

	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new HadoopELT(), args);
		System.exit(exitCode);

	}

}
