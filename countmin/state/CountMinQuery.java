
package storm.starter.trident.project.countmin.state;

import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.TridentCollector;
import java.util.List;
import java.util.ArrayList;
import backtype.storm.tuple.Values;


/**
 *@author: Shitian Shen (sshen@ncsu.edu)
 */


public class CountMinQuery extends BaseQueryFunction<TopList, String> {
    public List<String> batchRetrieve(TopList state, List<TridentTuple> inputs) {
	List<String> ret = new ArrayList();        
	List<String> temp = new ArrayList();    
	//append the top K twitter words and their count to the result.
	for(Object input: state.getAll().toArray()) {
	    temp.add( input.toString() + ":" + String.valueOf(state.estimateCount(input.toString())) );
        }
	for(TridentTuple input:inputs){
		ret.add(String.valueOf(temp));	
	}
        return ret;
    }

    public void execute(TridentTuple tuple, String count, TridentCollector collector) {
        collector.emit(new Values(count));
    }    
}
