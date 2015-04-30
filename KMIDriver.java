import java.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

public class KMIDriver extends Configured implements Tool
{
    public static int minimum = 0;
    public static int maximum = 0;
    public int run (String args[])  throws IOException
    {
        JobConf conf = new JobConf (KMIDriver.class);
        conf.setJobName ("KMI");
        
        // TODO
        conf.setMapperClass (KMIMapper.class);
        conf.setReducerClass (KMIReducer.class);
        // reducer for first step is associative and commutative
        conf.setCombinerClass (KMIReducer.class);
        
        conf.setOutputKeyClass (Text.class);
        conf.setOutputValueClass (IntWritable.class);
        
        conf.setInputFormat (TextInputFormat.class);
        conf.setOutputFormat (TextOutputFormat.class);
        
        FileInputFormat.setInputPaths (conf, new Path (args[0]));
        FileOutputFormat.setOutputPath (conf, new Path (args[1]));
        
        JobClient.runJob (conf);
        
        return 0;
    }
    
    public static void main (String[] args) throws Exception
    {
        File file = new File ("MAX_MIN");
        if (!file.exists())
        {
            file.createNewFile ();
        }
        int exitCode = ToolRunner.run (new KMIDriver(), args);
        FileWriter fw = new FileWriter (file.getAbsoluteFile ());
        BufferedWriter bw = new BufferedWriter (fw);
        String str = Integer.toString (maximum) + "\t" + Integer.toString (minimum) + "\n";
        bw.write (str);
        bw.close ();
        System.exit (exitCode);
        
    } 
}
