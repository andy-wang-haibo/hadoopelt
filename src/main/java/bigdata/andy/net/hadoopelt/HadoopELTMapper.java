package bigdata.andy.net.hadoopelt;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HadoopELTMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private static int EMPLOYMENT_CODE_START = "SMU01266207072200001".length() - 2;
	private static String ALL_EMPLOYMENT_CODE = "01";
	private static int STATE_CODE_START = "SMU".length() -1;
	private static int STATE_CODE_LENGTH = 2;
	private static int SUPER_SECTOR_START = "SMU0126620".length();
	private static int SUPER_SECTOR_LENGTH = 2;
	
	private static int SERIES_ID_FIELD = 0;
	private static int EMPLOYMENT_DATA_FIELD = 3;
	
	private Map<String, String> stateMap;
	private Map<String, String> supersectorMap;

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		
		BufferedReader reader = Files.newBufferedReader(Paths.get("sm.state"));
		String line = null;
		while ((line = reader.readLine()) != null ) {
			String[] parts = line.split("\\s+");
			stateMap.put(parts[0], parts[1]);
		}
		
		reader = Files.newBufferedReader(Paths.get("sm.supersector"));
		while ( (line = reader.readLine()) != null ) {
			String[] parts = line.split("\\s+");
			supersectorMap.put(parts[0], parts[1]);
		}
	}


	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// I just want employment data. Will Skip the rest.
		String line = value.toString();
		String[] parts = line.split("\\s+");
		if (parts[SERIES_ID_FIELD].endsWith(ALL_EMPLOYMENT_CODE) && 
				( parts.length > 4 && !parts[EMPLOYMENT_DATA_FIELD].contains("-"))) {
			String stateCode = line.substring(STATE_CODE_START, STATE_CODE_LENGTH);
			String supersectorCode = line.substring(SUPER_SECTOR_START, SUPER_SECTOR_LENGTH);
			
			String state = stateMap.get(stateCode);
			String supersector = supersectorMap.get(supersectorCode);
			String year = parts[1];
			String month = parts[2];
			String empData = parts[3];
			
			String output = String.join(",", state, supersector, year, month, empData);
			
			context.write(new Text(state), new Text(output));
			
		}
		
		
	}
	


}
