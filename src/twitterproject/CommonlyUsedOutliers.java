package twitterproject;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import org.bson.Document;

/**
 * Finds the commonly used items in our database
 *
 * @author achilles
 */
public class CommonlyUsedOutliers
{

	//database fields
	private final MongoClient mongoClient;
	private final MongoDatabase db;
	private final MongoCollection<Document> users;

	//count maps
	private final CountMap<String> urls;
	private final CountMap<String> mentions;
	private final CountMap<String> hashtags;
	private final CountMap<String> retweets;

	//sets containing the outliers 
	private final HashSet<String> outlierUrls;
	private final HashSet<String> outlierMentions;
	private final HashSet<String> outlierHashtags;
	private final HashSet<String> outlierRetweets;

	public CommonlyUsedOutliers()
	{
		//initialize fields and connect to database
		this.mongoClient = new MongoClient("127.0.0.1", 27017);
		this.db = mongoClient.getDatabase("Twitter");
		this.users = db.getCollection("userTable");
		this.urls = new CountMap<>();
		this.mentions = new CountMap<>();
		this.hashtags = new CountMap<>();
		this.retweets = new CountMap<>();
		this.outlierUrls = new HashSet<>();
		this.outlierMentions = new HashSet<>();
		this.outlierHashtags = new HashSet<>();
		this.outlierRetweets = new HashSet<>();
	}

	public HashSet<String> getOutlierUrls()
	{
		return this.outlierUrls;
	}

	public HashSet<String> getOutlierMentions()
	{
		return this.outlierMentions;
	}

	public HashSet<String> getOutlierHashtags()
	{
		return this.outlierHashtags;
	}

	public HashSet<String> getOutlierRetweets()
	{
		return this.outlierRetweets;
	}

	/**
	 * Examines the database and reports outliers
	 */
	public void find()
	{
		//save everything related to every user  
		for (Document user : users.find())
		{
			findTokens(user, "mentions", "mention", this.mentions);
			findTokens(user, "hashtags", "hashtag", this.hashtags);
			findTokens(user, "urls", "url", this.urls);
			findTokens(user, "retweets", "id", this.retweets);
		}

		//remove entities that appear only once (marked as outliers)
		//such entities are not useful because they do not create links between users. 
		/*
		this.outlierUrls.addAll(this.urls.removeValues(1));
		this.outlierMentions.addAll(this.mentions.removeValues(1));
		this.outlierHashtags.addAll(this.hashtags.removeValues(1));
		this.outlierRetweets.addAll(this.retweets.removeValues(1));
		 */
		this.urls.removeValues(1);
		this.mentions.removeValues(1);
		this.hashtags.removeValues(1);
		this.retweets.removeValues(1);

		//sort each category based on frequency (boxplot input format)
		ArrayList<Entry<Integer, String>> mentionSorted = this.mentions.getValuesSorted();
		ArrayList<Entry<Integer, String>> hashtagSorted = this.hashtags.getValuesSorted();
		ArrayList<Entry<Integer, String>> urlSorted = this.urls.getValuesSorted();
		ArrayList<Entry<Integer, String>> retweetSorted = this.retweets.getValuesSorted();

		//create boxplots
		BoxPlot mentionBox = new BoxPlot(mentionSorted);
		BoxPlot hashtagBox = new BoxPlot(hashtagSorted);
		BoxPlot urlBox = new BoxPlot(urlSorted);
		BoxPlot retweetBox = new BoxPlot(retweetSorted);

		//filter entities and define which ones are outliers
		filterList(mentionSorted, mentionBox, this.outlierMentions);
		filterList(hashtagSorted, hashtagBox, this.outlierHashtags);
		filterList(urlSorted, urlBox, this.outlierUrls);
		filterList(retweetSorted, retweetBox, this.outlierRetweets);

	}

	/**
	 * Filters a list, based on a boxplot, and finds which entities are upper
	 * outliers
	 *
	 * @param list a list containing entries with frequency as a key and entity
	 * as a value
	 * @param box a boxplox, capable of classifying an entity (outlier or not)
	 * @param set a set in which we save outliers
	 */
	private void filterList(List<Entry<Integer, String>> list, BoxPlot box, HashSet<String> set)
	{
		for (Entry<Integer, String> entry : list)
		{
			Integer key = entry.getKey();
			String value = entry.getValue();
			if (box.isOutlierUpper(key))
			{
				set.add(value);
			}
		}
	}

	/**
	 * Finds specific entities for a document and saves them
	 *
	 * @param document the document to be checked
	 * @param field the category to be searched
	 * @param id the name of the field containing the information needed
	 * @param map the countmap in which information is saved
	 */
	private void findTokens(Document document, String field, String id, CountMap map)
	{
		ArrayList<Document> tokens = (ArrayList<Document>) document.get(field);
		if (tokens != null)
		{
			for (Document token : tokens)
			{
				String str = String.valueOf(token.get(id));
				//some urls contained more than 1011 characters. They couldnt be saved correctly due to database restrictions
				//we define them as outliers as they are invalid urls
				if (field.equals("urls"))
				{
					if (str.length() > 1011)
					{
						outlierUrls.add(str);
					}
				}
				map.add(str);
			}
		}
	}

}
