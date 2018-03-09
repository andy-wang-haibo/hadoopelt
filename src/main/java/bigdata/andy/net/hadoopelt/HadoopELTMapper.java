package bigdata.andy.net.hadoopelt;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HadoopELTMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static String ALL_EMPLOYMENT_CODE = "01";
	private static int STATE_CODE_START = "SMU".length();
	private static int STATE_CODE_LENGTH = 2;
	private static int SUPER_SECTOR_START = "SMU0126620".length();
	private static int SUPER_SECTOR_LENGTH = 2;

	private static int SERIES_ID_FIELD = 0;
	private static int EMPLOYMENT_DATA_FIELD = 3;

	private Map<String, String> stateMap = new HashMap<>();
	private Map<String, String> supersectorMap = new HashMap<>();

	Text outputKey = new Text();
	Text outputValue = new Text();

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);

		BufferedReader reader = Files.newBufferedReader(Paths.get("sm.state"));
		String line = null;
		while ((line = reader.readLine()) != null) {
			String[] parts = line.split("\\s+", 2);
			stateMap.put(parts[0], parts[1].trim());
		}

		reader = Files.newBufferedReader(Paths.get("sm.supersector"));
		while ((line = reader.readLine()) != null) {
			String[] parts = line.split("\\s+", 2);
			supersectorMap.put(parts[0], parts[1].trim());
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// I just want employment data. Will Skip the rest.
		String line = value.toString();
		System.err.println(line);
		String[] parts = line.split("\\s+");
		if (parts[SERIES_ID_FIELD].endsWith(ALL_EMPLOYMENT_CODE)
				&& (parts.length > 3 && !parts[EMPLOYMENT_DATA_FIELD].contains("-"))) {
			String stateCode = line.substring(STATE_CODE_START, STATE_CODE_START + STATE_CODE_LENGTH);
			String supersectorCode = line.substring(SUPER_SECTOR_START, SUPER_SECTOR_START + SUPER_SECTOR_LENGTH);

			String state = stateMap.get(stateCode);
			System.err.format("State: %s\n", state);
			if (state == null ) return;
			String supersector = supersectorMap.get(supersectorCode);
			String year = parts[1].trim();
			String month = parts[2].trim();
			String empData = parts[3].trim();

			String output = String.join(",", state, supersector, year, month, empData);
			
			System.err.println(output);
			outputKey.set(state);
			outputValue.set(output);
			context.write(outputKey, outputValue);

		}

	}

}
