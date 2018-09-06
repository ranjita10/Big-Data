import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

public class TFIDFGetWordReducer extends Reducer<Text, Text, Text, Text> {



    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

        TreeMap<Double, String> tmap = new TreeMap<Double, String>();

        for (Text text : value) {
            String[] valueList = text.toString().split("\t");
            tmap.put(-Double.parseDouble(valueList[0]), valueList[1]);
        }

        int numberOfWord = 10;

        int currentWord = 0;

        String result = "";

        for(Map.Entry<Double, String> entry : tmap.entrySet()) {
            if (currentWord < numberOfWord) {
                currentWord++;
                result = result + " " + entry.getValue();
            } else {
                break;
            }

        }

        context.write(new Text(result), new Text(""));







    }
}
