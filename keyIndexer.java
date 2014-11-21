package keywords;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class keyIndexer {

  public static String keyline;
  public static String[] keylist= new String[100];
  public static class keyIndexMapper extends MapReduceBase
      implements Mapper<LongWritable, Text, Text, Text> {

    private final static Text word = new Text();
    private final static Text location = new Text();

    public void map(LongWritable key, Text val,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      
      //read the Inverted index file as input
      String line = val.toString();
      String[] itr = line.split("\\s+");
      //store the first words i.e the term in the inverted index
      word.set(itr[0]);
      
      //keylist holds the list of keywords
      for(String ele : keylist){
          
    	  //TODO check for substrings
          if(ele.equals(word.toString())){  //if the word belongs to the keylist
              for(int i=1;i<itr.length;i++) {
                location.set(itr[i]);
                output.collect(location,word);
                //output in the format of book-> word1, word2 ...
          }
     }
    }
   }
  }


  public static class keyIndexReducer extends MapReduceBase
      implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {

      boolean first = true;
      StringBuilder toReturn = new StringBuilder();
      while (values.hasNext()){
        if (!first)
          toReturn.append(", ");
        first=false;
        toReturn.append(values.next().toString());
      }

      output.collect(key, new Text(toReturn.toString()));
    }
  }


  /**
   * The actual main() method for our program; this is the
   * "driver" for the MapReduce job.
   */
  public static void main(String[] args) {
    JobClient client = new JobClient();
    JobConf conf = new JobConf(keyIndexer.class);

    conf.setJobName("keyIndexer");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    //to get the keywords
    FileSystem fs;
	try {
	    // to read the keywords from the file 'keyIP' stored in folder 'keys'
		fs = FileSystem.get(conf);
		Path keyfile= new Path(new Path("keys"),"keyIP");
		if(!fs.exists(keyfile))
		    throw new IOException("Keywords file not found");
		BufferedReader br = null;
		br = new BufferedReader (new InputStreamReader(fs.open(keyfile)));
		
	    while((keyline= br.readLine())!= null){
	        keylist = keyline.split("\\s+");
	        System.out.println("hello there do something !!!"+keylist[0]+keylist[1]);
	        
	    }
	    // the input here is the output of the inverted indexer stored in 'output' , filename 'part-00000'
	    FileInputFormat.addInputPath(conf, new Path("output/part-00000"));
	    FileOutputFormat.setOutputPath(conf, new Path("keyOP"));

	    conf.setMapperClass(keyIndexMapper.class);
	    conf.setReducerClass(keyIndexReducer.class);

	    client.setConf(conf);

	    try {
	      JobClient.runJob(conf);
	    } catch (Exception e) {
	      e.printStackTrace();
	    }

	} catch (IOException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
   
   }
}

