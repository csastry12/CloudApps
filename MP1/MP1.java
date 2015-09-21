import java.io.BufferedReader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class MP1 {
    Random generator;
    String userName;
    String inputFileName;
    String delimiters = " \t,;.?!-:@[](){}_*/";
    String[] stopWordsArray = {"i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours",
            "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its",
            "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that",
            "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having",
            "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while",
            "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before",
            "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again",
            "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each",
            "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than",
            "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"};

    void initialRandomGenerator(String seed) throws NoSuchAlgorithmException {
        MessageDigest messageDigest = MessageDigest.getInstance("SHA");
        messageDigest.update(seed.toLowerCase().trim().getBytes());
        byte[] seedMD5 = messageDigest.digest();

        long longSeed = 0;
        for (int i = 0; i < seedMD5.length; i++) {
            longSeed += ((long) seedMD5[i] & 0xffL) << (8 * i);
        }

        this.generator = new Random(longSeed);
    }

    Integer[] getIndexes() throws NoSuchAlgorithmException {
        Integer n = 10000;
        Integer number_of_lines = 50000;
        Integer[] ret = new Integer[n];
        this.initialRandomGenerator(this.userName);
        for (int i = 0; i < n; i++) {
            ret[i] = generator.nextInt(number_of_lines);
        }
        return ret;
    }

    public MP1(String userName, String inputFileName) {
        this.userName = userName;
        this.inputFileName = inputFileName;
    }

    public String[] process() throws Exception {
        String[] ret = new String[20];
        
        Integer[] indexes = getIndexes();
       
        //TODO
        
        // First read in the data from the input file
        
        try
        {         
           FileReader fileReader = new FileReader(inputFileName);
           BufferedReader bufferedReader = new BufferedReader(fileReader);
           List<String> lines = new ArrayList<String>();
           List<String> linesToAnalyze = new ArrayList<String>();
           List<String> stopWordsArrayList = new ArrayList<String>();
           String line = null;
         //Map<String, Integer> wordMap = new HashMap<>();
           Map<String, Integer> wordMap = new TreeMap<>();
           
           while((line = bufferedReader.readLine()) != null)
           {
        	   lines.add(line); 
           }
           
           bufferedReader.close();
           
           for (int i = 0; i < indexes.length; i++) 
           {
        	   linesToAnalyze.add(lines.get(indexes[i]));
           }
           
           // create array list of stop words
           
           for (int i = 0; i < stopWordsArray.length; i++) 
           {
        	   stopWordsArrayList.add(stopWordsArray[i]);
           } 
                
           String[] linesRead = linesToAnalyze.toArray(new String[linesToAnalyze.size()]);
           
           for (String line1 : linesRead)
           {
        	   StringTokenizer st = new StringTokenizer(line1, delimiters);
        	   
        	   while (st.hasMoreTokens()) 
        	   {
        		   String token = st.nextToken().trim().toLowerCase();
        		   if (!stopWordsArrayList.contains(token))
        		   {
        			   	 // if the map contains the word
        			   
        		         if (wordMap.containsKey(token)) // is word in map
        		         {
        		            int count = wordMap.get(token); // get current count
        		            wordMap.put(token, count + 1); // increment count
        		         } 
        		         else 
        		        	 wordMap.put(token, 1); // add new word with a count of 1 to map
        		   }
        	   }
           }
           
           Map<String, Integer> wordCountSorted =  sortByComparator(wordMap);
           
    //       System.out.printf("%nSorted Map contains:%nKey\t\tValue%n");
           int count = 0;
           
           for (Map.Entry entry : wordCountSorted.entrySet())
           {
        // 	  System.out.printf("%-10s%10s%n", entry.getKey().toString(), entry.getValue());
        	  ret[count] = entry.getKey().toString();
         	  count++;
         	  
         	  if (count == 20)
         	  {
         		  break;
         	  }
           }
        } 
        catch (IOException ioException)
        {
           System.err.println("Error opening file. Terminating.");
           System.exit(1);
        } 

        return ret;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1){
            System.out.println("MP1 <User ID>");
        }
        else {
            String userName = args[0];
            String inputFileName = "./input.txt";
            MP1 mp = new MP1(userName, inputFileName);
            String[] topItems = mp.process();
            for (String item: topItems){
                System.out.println(item);
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
