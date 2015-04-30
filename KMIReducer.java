import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

public class KMIReducer extends MapReduceBase implements
    Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce (Text a_key, Iterator<IntWritable> values,
         OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException
            {
                Text key = a_key;
                int user_frequency_count = 0;
                while (values.hasNext())
                {
                    IntWritable value = (IntWritable)values.next ();
                    user_frequency_count += value.get ();
                }
                
                if (KMIDriver.maximum < user_frequency_count)
                    KMIDriver.maximum = user_frequency_count;
                if (KMIDriver.minimum > user_frequency_count)
                    KMIDriver.minimum = user_frequency_count;
                output.collect (key, new IntWritable(user_frequency_count));
            }
    }
