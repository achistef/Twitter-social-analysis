package twitterproject;

import twitter4j.FilterQuery;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 *
 * @author achilles
 */
public class StreamConsumer
{

	public static void main(String[] args)
	{
		ConfigurationBuilder confBuilder = new ConfigurationBuilder();
		confBuilder.setDebugEnabled(true);
		confBuilder.setJSONStoreEnabled(true);
		confBuilder.setOAuthConsumerKey("insert your key");
		confBuilder.setOAuthConsumerSecret("insert your key");
		confBuilder.setOAuthAccessToken("insert your key");
		confBuilder.setOAuthAccessTokenSecret("");
		TwitterStream twitterStream = new TwitterStreamFactory(confBuilder.build()).getInstance();
		twitterStream.clearListeners();
		StatusListener statusListener = new StreamListener(twitterStream);
		
		FilterQuery filters = new FilterQuery();
		String keywords[] =
		{
			"debate", "trump", "win", "America", "election", "hillary", "Canada", "Greece", "news", "Obama"
		};
		filters.track(keywords);
		twitterStream.addListener(statusListener);
		twitterStream.filter(filters);
	}
}
