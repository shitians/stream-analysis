package storm.starter.trident.project.countmin; 

import java.util.Arrays;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.StateFactory;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;
import storm.trident.testing.FixedBatchSpout;
import backtype.storm.tuple.Values;


import storm.trident.operation.builtin.Count;
import storm.starter.trident.project.countmin.membership.BloomFilter;

//import extra functions,spouts,filters;
import storm.starter.trident.project.spouts.TwitterSampleSpout;
import storm.starter.trident.project.functions.ParseTweet;
import storm.starter.trident.project.functions.SentenceBuilder;
import storm.starter.trident.project.filters.Print;
import storm.starter.trident.project.filters.PrintFilter;
import storm.starter.trident.project.functions.Tweet;
import storm.starter.trident.project.filters.BFilter;
import storm.trident.testing.FixedBatchSpout;

import storm.starter.trident.project.countmin.state.CountMinSketchStateFactory;
import storm.starter.trident.project.countmin.state.CountMinQuery;
import storm.starter.trident.project.countmin.state.CountMinSketchUpdater;
import storm.starter.trident.tutorial.functions.SplitFunction;

/**
 *@author: Shitian Shen (sshen@ncsu.edu)
 */


public class CountMinSketchTopology {

	 public static StormTopology buildTopology(String[] args, LocalDRPC drpc ) {

        TridentTopology topology = new TridentTopology();

        int width = 1000;
		int depth = 15;
		int seed = 10;
		int topK = 5;
        
    	/***FixedBatchSpout spoutFixedBatch = new FixedBatchSpout(new Fields("sentence"), 3,
	 		new Values("the cow jumped over the moon"),
			new Values("the man went to the store and bought some candy"),
			new Values("four score and seven years ago"),
			new Values("how many apples can you eat"),
			new Values("to be or not to be the person"))
			;
	 	spoutFixedBatch.setCycle(false);***/

	

	/**Create Spouts**/
	//Twitter's account credentials passed as args

	String consumerKey = args[0];
    String consumerSecret = args[1];
    String accessToken = args[2];
    String accessTokenSecret = args[3];
    
    /*
	consumerKey = "csNXZW0Jhd6JoQNSAraUC8crJ";
	consumerSecret = "iHSOD9lm7OJGhnqfiiZf371SXzsLD7UaqKhlqMy0MQX6MDfQcH";
	accessToken = "555507187-4pwfiHkb50sNaOwaprxdUR67eecxAsMQ7co9GFBg";
	accessTokenSecret = "1LZxVtHpndxNHs6LuUQ0A5sH7GaH5z1PmNYhoJlu41hUF";
	*/

	/* user can modify the number of topK by using command input*/
	if(args.length == 5){
		String temp = args[4];
		topK = Integer.parseInt(temp);
	}

	String[] arguments = args.clone();
	String[] topicWords = Arrays.copyOfRange(arguments, 0, arguments.length);
	TwitterSampleSpout spoutTweets = new TwitterSampleSpout(consumerKey, consumerSecret,accessToken, accessTokenSecret, topicWords);

		TridentState countMinDBMS = 
			topology.newStream("tweets", spoutTweets)
			.parallelismHint(1)
			.each(new Fields("tweet"),new ParseTweet(),new Fields("text","tweetId","user"))
			//.each(new Fields("text","tweetId","user"), new PrintFilter("PARSED TWEETS:"))
			.each(new Fields("text", "tweetId", "user"), new SentenceBuilder(), new Fields("sentence"))
			.each(new Fields("sentence"), new Split(), new Fields("words"))
			// filter stop word.
			.each(new Fields("words"), new BFilter())
			.partitionPersist( new CountMinSketchStateFactory(depth,width,seed,topK), new Fields("words"), new CountMinSketchUpdater())
			.parallelismHint(1)
			;


		topology.newDRPCStream("get_topK", drpc)
			//.each( new Fields("args"), new Split(), new Fields("query"))
			//.each(new Fields("args"), new PrintFilter())
			.stateQuery(countMinDBMS,new Fields("args"),new CountMinQuery(), new Fields("count"))
			.project(new Fields("count"))
			;

		return topology.build();

	}


	public static void main(String[] args) throws Exception {
			Config conf = new Config();
        	conf.setDebug( false );
        	conf.setMaxSpoutPending( 10 );


        	LocalCluster cluster = new LocalCluster();
        	LocalDRPC drpc = new LocalDRPC();
        	cluster.submitTopology("get_topK", conf, buildTopology(args,drpc));

        	for (int i = 0; i < 100; i++) {
            		System.out.println("DRPC RESULT:"+ drpc.execute("get_topK","topk"));
            		Thread.sleep( 1000 );
        	}

		System.out.println("STATUS: OK");
		//cluster.shutdown();
        //drpc.shutdown();
	}
}
