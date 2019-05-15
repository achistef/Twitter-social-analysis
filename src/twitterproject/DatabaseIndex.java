package twitterproject;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.IndexOptions;

/**
 *
 * @author achilles
 */
public class DatabaseIndex
{

	public static void main(String[] args)
	{
		MongoClientOptions clientOptions = MongoClientOptions
				.builder()
				.writeConcern(WriteConcern.UNACKNOWLEDGED)
				.build();
		MongoClient mongoClient = new MongoClient("127.0.0.1", clientOptions);

		com.mongodb.client.MongoDatabase db = mongoClient.getDatabase("Twitter");

		IndexOptions options = new IndexOptions();
		options.unique(true);
		db.getCollection("mentionTable").createIndex(new BasicDBObject("id", 1), options);
		db.getCollection("hashtagTable").createIndex(new BasicDBObject("id", 1), options);
		db.getCollection("urlTable").createIndex(new BasicDBObject("id", 1), options);
		db.getCollection("similarity").createIndex(new BasicDBObject("id1",1).append("id2", 1), options);
		
		
		mongoClient.close();
	}

}
