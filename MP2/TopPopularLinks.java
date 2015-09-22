import org.apache.commons.logging.Log;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

/*import TopTitles.TextArrayWritable;
import TopTitles.TitleCountMap;
import TopTitles.TitleCountReduce;
import TopTitles.TopTitlesMap;
import TopTitles.TopTitlesReduce;*/

import java.io.IOException;
import java.lang.Integer;
import java.util.StringTokenizer;
import java.util.TreeSet;

// >>> Don't Change
public class TopPopularLinks extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(TopPopularLinks.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopPopularLinks(), args);
        System.exit(res);
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }
// <<< Don't Change

    @Override
    public int run(String[] args) throws Exception 
    {
    	Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Top Popular Links");
        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(LinkCountMap.class);
        jobA.setReducerClass(LinkCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(TopPopularLinks.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Links");
        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(IntArrayWritable.class);

        jobB.setMapperClass(TopLinksMap.class);
        jobB.setReducerClass(TopLinksReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopPopularLinks.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> 
    {
        // TODO
    	
    	@Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
        	 // TODO
        	
    		String line = value.toString();
    		String[] tokens = line.split(": | ");

    		Integer nextToken = Integer.valueOf(tokens[0]);
    		context.write(new IntWritable(nextToken), new IntWritable(0));

    		for (int i = 1; i < tokens.length; i++)
    		{
    			nextToken = Integer.valueOf(tokens[i]);
    			context.write(new IntWritable(nextToken), new IntWritable(1));
    		}
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> 
    {
        // TODO
    	
    	@Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {	
    		int sum = 0;
            for (IntWritable val : values) 
            {
                sum += val.get();
            }
            
            context.write(key, new IntWritable(sum));
        }
    }

    public static class TopLinksMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        Integer N;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException 
        {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", N);
        }
        
        // TODO
        
        private TreeSet<Pair<Integer, Integer>> countToWordMap = new TreeSet<Pair<Integer, Integer>>();
        
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException 
        {
            // TODO
        	
        	Integer count = Integer.parseInt(value.toString());
            Integer word = Integer.parseInt(key.toString());
            
            // store count (# of links) and word (id of page) and sort by count

            countToWordMap.add(new Pair<Integer, Integer>(count, word));

            if (countToWordMap.size() > N) 
            {
                countToWordMap.remove(countToWordMap.first());
            }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException 
        {
            // TODO
        	
        	for (Pair<Integer, Integer> item : countToWordMap) 
            {
        		// send over (id, count) with count sorted
        		
                Integer[] strings = {item.second, item.first};
                IntArrayWritable val = new IntArrayWritable(strings);
                context.write(NullWritable.get(), val);
            }
        }
    }

    public static class TopLinksReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> 
    {
        Integer N;
        
        private TreeSet<Pair<Integer, Integer>> countToWordMap = new TreeSet<Pair<Integer, Integer>>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException 
        {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", N);
        }
        
        // TODO
        
        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException 
        {
            // TODO
        	
        	for (IntArrayWritable val: values) {
                Integer[] pair= (Integer[]) val.toArray();

            //    String word = pair[0].toString();
             //   Integer count = Integer.parseInt(pair[1].toString());
                
                // store (count, id) from all mappers and sort by count

                countToWordMap.add(new Pair<Integer, Integer>(pair[1], pair[0]));

                if (countToWordMap.size() > N) {
                    countToWordMap.remove(countToWordMap.first());
                }
        	}

            for (Pair<Integer, Integer> item: countToWordMap) 
            {
            	IntWritable word = new IntWritable(item.second);
                IntWritable value = new IntWritable(item.first);
                context.write(word, value);
            }
        }
    }
}

// >>> Don't Change
class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
// <<< Don't Change