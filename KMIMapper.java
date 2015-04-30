import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

public class KMIMapper extends MapReduceBase implements
    Mapper<LongWritable, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable (1);
        
        public void map (LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
        {
            String valueString = value.toString ();
            String[] follower = valueString.split("\t");
            output.collect (new Text (follower[0]), one);
        }
    }
