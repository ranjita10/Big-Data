import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;

import java.io.IOException;

public class DataCleanReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            String keyString = key.toString();

            keyString = keyString.replaceAll("\t", " ");


            context.write(new Text(keyString), value.iterator().next());
        
    }
}
