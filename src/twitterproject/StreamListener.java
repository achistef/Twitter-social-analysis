package twitterproject;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;

/**
 * Manages incoming tweets.
 * @author achilles
 */
public class StreamListener implements StatusListener
{

	private final TwitterStream twitterStream;
	private final MongoClient mongoClient;
	private final MongoDatabase db;
	private final MongoCollection<Document> streamTable;
	private int tweetCounter;
	//tweet limit
	private final int limit = 5000000;

	public StreamListener(TwitterStream twitterStream)
	{
		//connect to database.
		super();
		this.twitterStream = twitterStream;
		this.mongoClient = new MongoClient("127.0.0.1", 27017);
		this.db = mongoClient.getDatabase("Twitter");
		this.streamTable = db.getCollection("stream");
		this.tweetCounter = (int) this.streamTable.count();
	}

	private int nextCounter()
	{
		int temp = this.tweetCounter;
		this.tweetCounter++;
		return temp;
	}

	/**
	 * Checks if the tweet limit is reached
	 * @return 
	 */
	private boolean limitReached()
	{
		if (this.tweetCounter >= this.limit)
		{
			//close connections
			this.twitterStream.shutdown();
			this.mongoClient.close();
			return true;
		}
		return false;
	}

	@Override
	public void onStatus(Status status)
	{
		//get json
		String json = TwitterObjectFactory.getRawJSON(status);
		//save it to database
		Document document = new Document();
		document.put("_id", nextCounter());
		document.put("json", json);
		this.streamTable.insertOne(document);
		System.out.println(this.tweetCounter);
		if (limitReached())
		{
			System.exit(0);
		}
	}

	@Override
	public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice)
	{
		//it was not requested to implement this feature, which means we keep deleted tweets.
	}

	@Override
	public void onTrackLimitationNotice(int numberOfLimitedStatuses)
	{
		//keywords should not be changed, thus this method has no effect.
	}

	@Override
	public void onScrubGeo(long userId, long upToStatusId)
	{
		//not used
	}

	@Override
	public void onException(Exception ex)
	{
		//something went wrong...
		System.out.println(ex.getMessage());
	}

	@Override
	public void onStallWarning(StallWarning warning)
	{
		//it was not requested to implement this feature, which means we ignore stall warning
	}

}
