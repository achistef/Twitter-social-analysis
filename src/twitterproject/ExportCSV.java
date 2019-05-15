package twitterproject;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.PrintWriter;
import org.bson.Document;

/**
 * Saves the user tables to CSV files.
 * @author achilles
 */
public class ExportCSV
{

	public static void main(String[] args)
	{
		//connect to database
		MongoClientOptions clientOptions = MongoClientOptions
				.builder()
				.writeConcern(WriteConcern.UNACKNOWLEDGED)
				.build();
		MongoClient mongoClient = new MongoClient("127.0.0.1", clientOptions);

		MongoDatabase db = mongoClient.getDatabase("Twitter");

		MongoCollection<Document> similarity = db.getCollection("similarity");
		
		//create CSV files and open them with writers
		try (PrintWriter urlWriter = new PrintWriter("urls.csv", "UTF-8");
				PrintWriter mentionWriter = new PrintWriter("mentions.csv", "UTF-8");
				PrintWriter hashtagWriter = new PrintWriter("hashtags.csv", "UTF-8");
				PrintWriter retweetWriter = new PrintWriter("retweets.csv", "UTF-8");
				PrintWriter totalWriter = new PrintWriter("total.csv", "UTF-8"))
		{
			//define attributes
			urlWriter.println("source target weight type");
			mentionWriter.println("source target weight type");
			hashtagWriter.println("source target weight type");
			retweetWriter.println("source target weight type");
			totalWriter.println("source target weight type");
			Double temp;
			for (Document document : similarity.find())
			{
				//read ID
				Long id1 = (Long) document.get("id1");
				Long id2 = (Long) document.get("id2");

				//read similarities
				temp = (Double) document.get("url");
				Double url = temp > 0.1 ? temp : null;

				temp = (Double) document.get("mention");
				Double mention = temp > 0.1 ? temp : null;

				temp = (Double) document.get("hashtag");
				Double hashtag = temp > 0.1 ? temp : null;

				temp = (Double) document.get("retweet");
				Double retweet = temp > 0.1 ? temp : null;

				Double total = (Double) document.get("total");

				//save similarities to CSV files
				if (url != null)
				{
					urlWriter.println(id1 + " " + id2 + " " + url + " undirected");
				}

				if (mention != null)
				{
					mentionWriter.println(id1 + " " + id2 + " " + mention + " undirected");
				}

				if (hashtag != null)
				{
					hashtagWriter.println(id1 + " " + id2 + " " + hashtag + " undirected");
				}

				if (retweet != null)
				{
					retweetWriter.println(id1 + " " + id2 + " " + retweet + " undirected");
				}

				totalWriter.println(id1 + " " + id2 + " " + total + " undirected");

			}
		} catch (Exception e)
		{
			System.out.println("Something went wrong...");
		}
	}
}
