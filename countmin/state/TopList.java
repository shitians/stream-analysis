package storm.starter.trident.project.countmin.state;
import java.io.Serializable;
import java.util.PriorityQueue;
import storm.trident.state.State;

// Save the K top words in a PriorityQueue.
public class TopList implements State  {
		CountMinSketchState sketch;
		PriorityQueue heap;
		int topK = Integer.MAX_VALUE;
		
		public TopList(int topK,CountMinSketchState sketch){
			this.sketch = sketch;
			this.topK = topK;
			this.heap = new PriorityQueue(topK+1,new SketchComparator(sketch));
		}
		public void add(String element){
			//Add the count of the coming element in sketch; and if the element is not in PriorityQueue, add it to the queue.
			sketch.add(element,1);
			if(!heap.contains(element)){
				heap.add(element);}
			// Keep the queue always with size k
			while(heap.size()> topK) heap.remove();
		}

		public PriorityQueue getAll(){
			return heap;
		}
   
   // Query how much count of certain word.
   public long estimateCount(String item) {
        return sketch.estimateCount(item);
    }

 
   @Override
    public void beginCommit(Long txid) {
        return;
    }

    @Override
    public void commit(Long txid) {
        return;
    }
}

