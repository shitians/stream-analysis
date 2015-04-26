package storm.starter.trident.project.countmin.state;
import java.io.Serializable;
import java.util.Comparator;


public class SketchComparator implements Comparator{
	CountMinSketchState sketch;
	public SketchComparator(CountMinSketchState sketch){
		this.sketch = sketch;
	}
	public int compare(Object o1, Object o2){
		return (int)(sketch.estimateCount(o1.toString())-sketch.estimateCount(o2.toString()));
	}
}

