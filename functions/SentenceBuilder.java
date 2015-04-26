package storm.starter.trident.project.functions;

import backtype.storm.tuple.Values;
import com.google.common.base.Splitter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;


/**
 * @author Shitian Shen (sshen@ncsu.edu)
 */
public class SentenceBuilder extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
    	String text = tuple.getString(0);

        collector.emit(new Values(text));
    }
}

