package twitterproject;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.bson.Document;

/**
 *
 * @author achilles
 */
public class Similarity
{
	//sets containing outliers
	private Set<String> outlierUrls;
	private Set<String> outlierMentions;
	private Set<String> outlierHashtags;
	private Set<String> outlierRetweets;

	/**
	 * Constructor.
	 */
	public Similarity()
	{
		//find outliers and save them
		CommonlyUsedOutliers outliers = new CommonlyUsedOutliers();
		outliers.find();
		this.outlierUrls = outliers.getOutlierUrls();
		this.outlierMentions = outliers.getOutlierMentions();
		this.outlierHashtags = outliers.getOutlierHashtags();
		this.outlierRetweets = outliers.getOutlierRetweets();
	}

	public void setOutlierMentions(Set<String> set)
	{
		this.outlierMentions = set;
	}

	public void setOutlierHashtags(Set<String> set)
	{
		this.outlierHashtags = set;
	}

	public void setOutlierUrls(Set<String> set)
	{
		this.outlierUrls = set;
	}

	public void setOutlierRetweets(Set<String> set)
	{
		this.outlierRetweets = set;
	}

	public Set<String> getOutlierMentions()
	{
		return this.outlierMentions;
	}

	public Set<String> getOutlierHashtags()
	{
		return this.outlierHashtags;
	}

	public Set<String> getOutlierUrls()
	{
		return this.outlierUrls;
	}

	public Set<String> getOutlierRetweets()
	{
		return this.outlierRetweets;
	}

	/**
	 * Finds the cosine similarities between two users and saves them to a tree map
	 * @param user1 first user
	 * @param user2 second user
	 * @param treeMap the tree map in which results are saved
	 */
	public void findSimilarities(UserDocument user1, UserDocument user2, MyTreeMap<Double, Document> treeMap)
	{
		//create a document containing the information needed
		Document document = new Document();
		document.append("id1", user1.getID());
		document.append("id2", user2.getID());

		//urls
		double d = user1.getAccUrls() * user2.getAccUrls();
		double cosineU = (d > 0) ? cosineSimilarity(user1.getUrlMap(), user2.getUrlMap(), d) : 0;
		if (cosineU >= 0)
		{
			document.append("url", cosineU);
		}

		//mentions
		d = user1.getAccMentions() * user2.getAccMentions();
		double cosineM = (d > 0) ? cosineSimilarity(user1.getMentionMap(), user2.getMentionMap(), d) : 0;
		if (cosineM >= 0)
		{
			document.append("mention", cosineM);
		}

		//hashtags
		d = user1.getAccHashtags() * user2.getAccHashtags();
		double cosineH = (d > 0) ? cosineSimilarity(user1.getHashtagMap(), user2.getHashtagMap(), d) : 0;
		if (cosineH >= 0)
		{
			document.append("hashtag", cosineH);
		}

		//retweets
		d = user1.getAccRetweets() * user2.getAccRetweets();
		double cosineR = (d > 0) ? cosineSimilarity(user1.getRetweetMap(), user2.getRetweetMap(), d) : 0;
		if (cosineR >= 0)
		{
			document.append("retweet", cosineR);
		}

		double cosine = cosineH * 0.25 + cosineM * 0.25 + cosineR * 0.25 + cosineU * 0.25;
		if (cosine >= 0)
		{
			document.append("total", cosine);
			//add to tree map
			treeMap.put(cosine, document);
		}

	}

	/**
	 * Transforms a list. 
	 * Given a list of documents, it creates a list of strings. 
	 * @param list the list to be transformed
	 * @param field the field containing the string needed
	 * @return a list of strings
	 */
	public List<String> transformList(List<Document> list, String field)
	{
		ArrayList<String> result = new ArrayList<>();
		for (Document document : list)
		{
			result.add(String.valueOf(document.get(field)));
		}
		return result;
	}

	/**
	 * Calculates the cosine similarity between two users.
	 * @param map1 a map containing the first user's entities
	 * @param map2 a map containing the second user's entities.
	 * @param d the pre calculated denominator of the formula
	 * @return the cosine similarity between the users
	 */
	private double cosineSimilarity(CountMap map1, CountMap map2, double d)
	{
		int dot = map1.dotProduct(map2);
		return dot / d;

	}

}
