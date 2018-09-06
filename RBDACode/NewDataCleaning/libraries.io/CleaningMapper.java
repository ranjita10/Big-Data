import java.io.IOException;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.commons.csv.*;

import java.io.*;
import java.util.*;
import java.lang.*;
import java.util.regex.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//import com.opencsv.*;
import au.com.bytecode.opencsv.CSVParser;

public class CleaningMapper
	extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) 
		throws IOException, InterruptedException {
			if(key.get() > 0){
				try{
					CSVParser parser = new CSVParser();
					String record = value.toString();
					
					
					String[] records = parser.parseLine(record);

					String[] column_name = {"ID","Platform","Name","Created Timestamp","Updated Timestamp",
					"Description","Keywords","Homepage URL","Licenses","Repository URL","Versions Count",
					"SourceRank","Latest Release Publish Timestamp","Latest Release Number","Package Manager ID",
					"Dependent Projects Count","Language","Status","Last synced Timestamp","Dependent Repositories Count",
					"Repository ID","Repository Host Type","Repository Name with Owner","Repository Description",
					"Repository Fork?","Repository Created Timestamp","Repository Updated Timestamp",
					"Repository Last pushed Timestamp","Repository Homepage URL","Repository Size",
					"Repository Stars Count","Repository Language","Repository Issues enabled?","Repository enabled?",
					"Repository Pages enabled?","Repository Forks Count","Repository Mirror URL","Repository Open Issues Count",
					"Repository Default branch","Repository Watchers Count","Repository UUID",
					"Repository Fork Source Name with Owner","Repository License","Repository Contributors Count","Repository Readme filename",
					"Repository Changelog filename","Repository Contributing guidelines filename",
					"Repository License filename","Repository Code of Conduct filename",
					"Repository Security Threat Model filename","Repository Security Audit filename","Repository Status",
					"Repository Last Synced Timestamp","Repository SourceRank","Repository Display Name","none","Repository SCM type",
					"Repository Pull requests enabled?", "Repository Logo URL","Repository Keywords"};

					if(records[23].equals("") || records[25].equals("")) {

					} else {
						context.write(new Text(records[23]), new Text(records[25].substring(0, 4)));
					}
				}catch(IOException e){}
			}
			
    	}
}


