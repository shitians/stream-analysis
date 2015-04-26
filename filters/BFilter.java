package storm.starter.trident.project.filters;

import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import storm.starter.trident.project.countmin.membership.BloomFilter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;

/**
 * Print filter for printing Trident tuples. 
 * used in debugging
 *
 * @author Shitian Shen
 */

public class BFilter extends BaseFilter {

        /* hard code the stop words*/
    	public String StopWords[];
    	public BFilter() {
			StopWords= new String[] {"a","A","an","An","the","The","of","I","you"};
    	}

        @Override
        public boolean isKeep(TridentTuple tuple)  {
        	
        	BloomFilter bf = new BloomFilter(15, 0.02);

			for (int i=0; i<StopWords.length; i++) {
	    		bf.add(StopWords[i]);
			}

			String str = tuple.getString(0);
			return !bf.isPresent(str);
        }
}




