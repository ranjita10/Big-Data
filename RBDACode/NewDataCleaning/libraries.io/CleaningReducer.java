import java.io.IOException;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.util.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.lang.*;
import java.util.regex.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//import com.opencsv.*;

public class CleaningReducer
	extends Reducer<Text, Text, Text, Text> {
	@Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
	throws IOException, InterruptedException {
			String keyString = key.toString();

            keyString = keyString.replaceAll("\t", " ");


            context.write(new Text(keyString), values.iterator().next());


			
		
	}

	
}
