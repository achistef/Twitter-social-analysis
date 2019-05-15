package twitterproject;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

/**
 *
 * @author achilles
 */
public class test2
{

	public static void main(String[] args) throws Exception
	{
		MongoClientOptions clientOptions = MongoClientOptions
				.builder()
				.writeConcern(WriteConcern.UNACKNOWLEDGED)
				.build();
		MongoClient mongoClient = new MongoClient("127.0.0.1", clientOptions);
		
		MongoDatabase db = mongoClient.getDatabase("Twitter");
		db.drop();
		/*MongoCollection<Document> streamTable = db.getCollection("stream");
		MongoCollection<Document> userTable = db.getCollection("userTable");
		MongoCollection<Document> mentionTable = db.getCollection("mentionTable");
		MongoCollection<Document> hashtagTable = db.getCollection("hashtagTable");
		MongoCollection<Document> retweetTable = db.getCollection("retweetTable");
		MongoCollection<Document> urlTable = db.getCollection("urlTable");

		System.out.println("stream table : " + streamTable.count());
		for (Document x : streamTable.listIndexes())
		{
			//System.out.println(x);
		}

		System.out.println("user table : " + userTable.count());
		for (Document x : userTable.listIndexes())
		{
			//System.out.println(x);
		}

		System.out.println("url table : " + urlTable.count());
		for (Document x : urlTable.listIndexes())
		{
			//System.out.println(x);
		}

		System.out.println("mention table : " + mentionTable.count());
		for (Document x : mentionTable.listIndexes())
		{
			//System.out.println(x);
		}

		System.out.println("hashtag table : " + hashtagTable.count());
		for (Document x : hashtagTable.listIndexes())
		{
			//System.out.println(x);
		}

		System.out.println("retweet table : " + retweetTable.count());
		for (Document x : retweetTable.listIndexes())
		{
			//System.out.println(x);
		}
		*/
		
		mongoClient.close();
	}

}
