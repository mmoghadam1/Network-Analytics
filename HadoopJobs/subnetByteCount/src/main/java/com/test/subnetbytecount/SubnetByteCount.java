import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import org.apache.commons.net.util.SubnetUtils;
import org.apache.commons.net.util.SubnetUtils.SubnetInfo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.commons.io.IOUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.lang.Long;
import java.util.HashMap;

public class SubnetByteCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    static enum CountersEnum { INPUT_WORDS }

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    private boolean caseSensitive;
    private Set<String> patternsToSkip = new HashSet<String>();

    private Configuration conf;
    private BufferedReader fis;

    private String[] routerSubnetKeys = {"regent", "tcom", "humn", "resnet", "econ", "rest"};
    private String data;
    private String[] temp;
    HashMap<String, List<ArrayList<Long>>> subnetHash = new HashMap<String, List<ArrayList<Long>>>();
    //private List<SubnetInfo> subnets = new ArrayList<SubnetInfo>();;
    //private List<ArrayList<Long>> subnets = new ArrayList<ArrayList<Long>>();
    @Override
    public void setup(Context context) throws IOException,
        InterruptedException {
      conf = context.getConfiguration();
      caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
      if (conf.getBoolean("wordcount.skip.patterns", false)) {
        URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
        for (URI patternsURI : patternsURIs) {
          Path patternsPath = new Path(patternsURI.getPath());
          String patternsFileName = patternsPath.getName().toString();
          parseSkipFile(patternsFileName);
        }
      }
 	for(int i = 0; i< routerSubnetKeys.length; i++){
	   subnetHash.put(routerSubnetKeys[i],loadSubnetData(conf, routerSubnetKeys[i]));
	}
    }

   private List<ArrayList<Long>> loadSubnetData(Configuration conf, String keyName){
      List<ArrayList<Long>> subnets = new ArrayList<ArrayList<Long>>();
      data = conf.get(keyName);
      StringTokenizer itr = new StringTokenizer(data,"\n");
      while (itr.hasMoreTokens()){
         ArrayList<Long> pair = new ArrayList<Long>();
         word.set(itr.nextToken());
         temp = word.toString().split(",",0);
         for(int i = 0; i < 2; i++){
            long ipNumbers = 0;
            int[] ip = new int[4];
            String[] parts = temp[i].trim().split("\\.");
            for (int j = 0; j < 4; j++) {
               ip[j] = Integer.parseInt(parts[j]);
               ipNumbers += ip[j] << (24 - (8 * j));
            }
            pair.add(ipNumbers);
         }
        subnets.add(pair);
      }
      return subnets;
   }

   private String checkSubnet(String ipaddr){
        for(int i = 0; i < routerSubnetKeys.length; i++){
	   List<ArrayList<Long>> subnets = subnetHash.get(routerSubnetKeys[i]);
	   for(int j = 0; j < subnets.size(); j++){
                long ipNumbers = 0;
                int[] ip = new int[4];
                String[] parts = ipaddr.trim().split("\\.");
                for (int k = 0; k < 4; k++) {
                        ip[k] = Integer.parseInt(parts[k]);
                        ipNumbers += ip[k] << (24 - (8 * k));
                }
                List<Long> pair = subnets.get(j);
                boolean test = ( (ipNumbers ^ pair.get(0)) & pair.get(1) ) == 0;
                if(test){
                   return routerSubnetKeys[i];
		}
           }
	}
        return "external"; 
    }

    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String line = (caseSensitive) ?
          value.toString() : value.toString().toLowerCase();
      for (String pattern : patternsToSkip) {
        line = line.replaceAll(pattern, "");
      }
      StringTokenizer itr = new StringTokenizer(line,",");

      word.set(itr.nextToken());
      String[] val = word.toString().trim().split(":");
      IntWritable bytes = new IntWritable(Integer.parseInt(val[0]));
      String srcSubnet = checkSubnet(val[1]);
      word.set(itr.nextToken());
      String destSubnet = checkSubnet(word.toString());
      String output = srcSubnet + "->" + destSubnet;;
      context.write(new Text(output),bytes);
      Counter counter = context.getCounter(CountersEnum.class.getName(),
    			        CountersEnum.INPUT_WORDS.toString());
      counter.increment(1);
    
    }

      private void parseSkipFile(String fileName) {
      try {
        fis = new BufferedReader(new FileReader(fileName));
        String pattern = null;
        while ((pattern = fis.readLine()) != null) {
          patternsToSkip.add(pattern);
        }
      } catch (IOException ioe) {
        System.err.println("Caught exception while parsing the cached file '"
            + StringUtils.stringifyException(ioe));
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void configureSubnets(Configuration conf){
    String[] keys = {"regent", "tcom", "humn", "resnet", "econ", "rest"};
    for(int i = 0; i < keys.length; i++){
       try{
          FileSystem fs = FileSystem.get(conf);
          Path inFile = new Path("/user/root/input/"+keys[i]+".txt");
          FSDataInputStream inputStream = fs.open(inFile);
          String value = IOUtils.toString(inputStream, "UTF-8");
          fs.close();
          conf.set(keys[i], value);
       }
       catch(IOException e) {
          e.printStackTrace();
       }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    configureSubnets(conf);

    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
      System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "subnet byte count");
    job.setJarByClass(SubnetByteCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    List<String> otherArgs = new ArrayList<String>();
    for (int i=0; i < remainingArgs.length; ++i) {
      if ("-skip".equals(remainingArgs[i])) {
        job.addCacheFile(new Path(remainingArgs[++i]).toUri());
        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
      } else {
        otherArgs.add(remainingArgs[i]);
      }
    }
    FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
