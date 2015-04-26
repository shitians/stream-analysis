package storm.starter.trident.project.countmin.state;

import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.TridentCollector;
import java.util.List;
import java.util.ArrayList;

/**
 *@author: Shitian Shen (sshen@ncsu.edu)
 */

public class CountMinSketchUpdater extends BaseStateUpdater<TopList> {

    public void updateState(TopList state, List<TridentTuple> tuples, TridentCollector collector) {
         
        for(TridentTuple t: tuples) { 
            state.add(t.getString(0));
        }
        
    }
}
