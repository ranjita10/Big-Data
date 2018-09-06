import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class DataCleanMapper extends Mapper<LongWritable, Text, Text, Text> {
private Text key;
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
String line = value.toString();

String bodyContent = getValueFromXML(line, "Body=");
String CreationDate = getValueFromXML(line,"CreationDate=");
if (CreationDate.length() < 4) {
	CreationDate = "-1";
} else {
	CreationDate = CreationDate.substring(0, 4);
}


context.write(new Text(bodyContent), new Text(CreationDate));

}

String getValueFromXML(String lineFromXML, String keyword) {
	

	int startIndex = lineFromXML.indexOf(keyword);
if (startIndex == -1) {
	return "";
}
	startIndex = startIndex+keyword.length();



	int endIndex = lineFromXML.indexOf("\"", startIndex+1);

	return lineFromXML.substring(startIndex+1, endIndex);

}


}
