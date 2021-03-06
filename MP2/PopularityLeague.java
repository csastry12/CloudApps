import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool 
{
//	static List<String> league;
	
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }
    
    // TODO
    
    @Override
    public int run(String[] args) throws Exception 
    {
    	Job job = Job.getInstance(this.getConf(), "Popularity League");
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(PopularityLeagueMap.class);
        job.setReducerClass(PopularityLeagueReduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(PopularityLeague.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        
     //   fs.close();
        return everything.toString();
    }
    
    public static class PopularityLeagueMap extends Mapper<Object, Text, IntWritable, IntWritable> 
    {
        // TODO
    	
    	List<String> league;
    	
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {

            Configuration conf = context.getConfiguration();

            String leaguePath = conf.get("league");

            this.league = Arrays.asList(readHDFSFile(leaguePath, conf).split("\n"));
        }
    	
    	@Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
        	 // TODO
        	
    		String line = value.toString();
    		String[] tokens = line.split(": | ");

   // 		Integer nextToken = Integer.valueOf(tokens[0]);
    		String nextToken = tokens[0];
    		if (league.contains(nextToken))
			{
    			context.write(new IntWritable(Integer.valueOf(nextToken)), new IntWritable(0));
			}

    		for (int i = 1; i < tokens.length; i++)
    		{
    		//	nextToken = Integer.valueOf(tokens[i]);
    			nextToken = tokens[i];
    			if (league.contains(nextToken))
    			{
    				context.write(new IntWritable(Integer.valueOf(nextToken)), new IntWritable(1));
    			}
    		}
        }
    }
    
    public static class PopularityLeagueReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> 
    {
        // TODO
    	
    	List<String> league;
    		
    	private HashMap<Integer, Integer> countToWordMap = new HashMap<>();
    	
    	@Override
        protected void setup(Context context) throws IOException,InterruptedException {

            Configuration conf = context.getConfiguration();

            String leaguePath = conf.get("league");

            this.league = Arrays.asList(readHDFSFile(leaguePath, conf).split("\n"));
        }
    	
    	@Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {	
    		int sum = 0;
            for (IntWritable val : values) 
            {
                sum += val.get();
            }
            
            countToWordMap.put(key.get(), sum);
            
     //       countToWordMap.remove(countToWordMap.firstEntry());
        }
    	
    	@Override
        protected void cleanup(Context context) throws IOException, InterruptedException
        {
            // TODO
    		
    	//	Map<Integer, Integer> countToWordMapSorted =  sortByComparator(countToWordMap);
        	
        	for (int i = 0; i < league.size(); i++) 
        	{
    			/*int leagueKey =  Integer.parseInt(league.get(i));
    		//	int keyVal = countToWordMapSorted.get(leagueKey);
    			int keyVal = countToWordMap.get(leagueKey);
    			int count = 0;
    			
    			for (int j = 0; j < league.size(); j++)
    			{
    				int leagueKeyCompare =  Integer.parseInt(league.get(j));
        		//	int keyValCompare = countToWordMapSorted.get(leagueKeyCompare);
    				int keyValCompare = countToWordMap.get(leagueKeyCompare);
    				if (keyValCompare < keyVal)
    				{
    					count++;
    				}
    			}
    			
    			context.write(new IntWritable(leagueKey), new IntWritable(count));*/
        		
        		context.write(new IntWritable(Integer.parseInt(league.get(i))), new IntWritable(1));
    		}
        	
        	for (Map.Entry entry : countToWordMap.entrySet())
            {
        		context.write(new IntWritable((Integer) entry.getKey()), new IntWritable((Integer) entry.getKey()));
            }
        }
    }
    
    private static Map sortByComparator(Map unsortMap) 
    {

        List list = new LinkedList(unsortMap.entrySet());

        // sort list based on comparator
        Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {
                return ((Comparable) ((Map.Entry) (o2)).getValue())
                        .compareTo(((Map.Entry) (o1)).getValue());
            }
        });

        // put sorted list into map again
        //LinkedHashMap make sure order in which keys were inserted
        Map sortedMap = new LinkedHashMap();
        for (Iterator it = list.iterator(); it.hasNext();) {
            Map.Entry entry = (Map.Entry) it.next();
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        return sortedMap;
    }
}
