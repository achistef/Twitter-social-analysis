package twitterproject;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a user.
 *
 * @author achilles
 */
public class UserDocument
{

	//user ID
	private final long id;
	//retweets accumulated
	private double accRetweets;
	//mentions accumulated
	private double accMentions;
	//urls accumulated
	private double accUrls;
	//hashtags accumulated
	private double accHashtags;
	//maps containing every entity associated with the used
	private final CountMap<String> retweetMap;
	private final CountMap<String> mentionMap;
	private final CountMap<String> urlMap;
	private final CountMap<String> hashtagMap;

	/**
	 * Constructor
	 *
	 * @param id the user ID
	 */
	public UserDocument(long id)
	{
		this.id = id;
		this.retweetMap = new CountMap<>();
		this.mentionMap = new CountMap<>();
		this.urlMap = new CountMap<>();
		this.hashtagMap = new CountMap<>();
		this.accRetweets = 0;
		this.accMentions = 0;
		this.accUrls = 0;
		this.accHashtags = 0;
	}

	public void setAccHashtags(double accHashtags)
	{
		this.accHashtags = accHashtags;
	}

	public long getID()
	{
		return this.id;
	}

	public CountMap<String> getRetweetMap()
	{
		return this.retweetMap;
	}

	public CountMap<String> getMentionMap()
	{
		return this.mentionMap;
	}

	public CountMap<String> getUrlMap()
	{
		return this.urlMap;
	}

	public CountMap<String> getHashtagMap()
	{
		return this.hashtagMap;
	}

	public double getAccRetweets()
	{
		return accRetweets;
	}

	public double getAccMentions()
	{
		return accMentions;
	}

	public double getAccUrls()
	{
		return accUrls;
	}

	public double getAccHashtags()
	{
		return accHashtags;
	}

	/**
	 * Calculates the accumulators and saves them in the class fields
	 */
	public void calcAccumulators()
	{
		this.accRetweets = this.retweetMap.accumulate();
		this.accMentions = this.mentionMap.accumulate();
		this.accUrls = this.urlMap.accumulate();
		this.accHashtags = this.hashtagMap.accumulate();
	}

	/**
	 * Examines if the user contains no entities
	 *
	 * @return
	 */
	public boolean isEmpty()
	{
		List<Integer> list = new ArrayList<>();
		list.add(this.hashtagMap.size());
		list.add(this.mentionMap.size());
		list.add(this.urlMap.size());
		list.add(this.retweetMap.size());
		for (Integer size : list)
		{
			if (size > 0)
			{
				return false;
			}
		}
		return true;
	}

	public int numOfEntities()
	{
		int result = 0;
		result += this.retweetMap.numOfItems();
		result += this.mentionMap.numOfItems();
		result += this.urlMap.numOfItems();
		result += this.hashtagMap.numOfItems();
		return result;
	}
}
