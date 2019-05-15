package twitterproject;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 *
 * @author achilles
 */
public class JsonProcessor
{

	private static MongoClient mongoClient;
	private static MongoDatabase db;
	private static MongoCollection<Document> streamTable;
	private static MongoCollection<Document> userTable;
	private static MongoCollection<Document> urlTable;
	private static MongoCollection<Document> mentionTable;
	private static MongoCollection<Document> hashtagTable;
	private static MongoCollection<Document> retweetTable;
	private static final Character OPENCURLYBRACKET = 0x7B;
	private static final Character CLOSECURLYBRACKET = 0x7D;
	private static final Character OPENBRACKET = 0x5B;
	private static final Character CLOSEBRACKET = 0x5D;
	private static final Character COLON = 0x3A;
	private static final Character DOUBLEQUOTATIONMARK = 0x22;
	private static final Character MINUS = 0x2D;
	private static final Character PLUS = 0x2B;
	private static final Character COMMA = 0x2C;
	private static final Character BACKSLASH = 0x5C;
	private static final Character DOT = 0x2E;
	private static final AtomicInteger HTTPREQ = new AtomicInteger();

	public static void main(String[] args) throws Exception
	{

		//connect to database
		MongoClientOptions options = MongoClientOptions
				.builder()
				.writeConcern(WriteConcern.UNACKNOWLEDGED)
				.build();
		mongoClient = new MongoClient("127.0.0.1", options);
		db = mongoClient.getDatabase("Twitter");
		streamTable = db.getCollection("stream");
		userTable = db.getCollection("userTable");
		urlTable = db.getCollection("urlTable");
		mentionTable = db.getCollection("mentionTable");
		hashtagTable = db.getCollection("hashtagTable");
		retweetTable = db.getCollection("retweetTable");
		long size = streamTable.count();
		//creating many threads is essential, since threads are sleeping while an http request is executed
		int numOfThreads = 100;
		final int threadLoad = (int) (size / numOfThreads);
		ExecutorService executor = Executors.newFixedThreadPool(numOfThreads);
		for (int i = 0; i < numOfThreads; i++)
		{
			final int counter = i;
			Thread thread = new Thread(new Runnable()
			{
				@Override
				public void run()
				{
					int block = threadLoad * counter;
					//each thread has its own workload
					for (int j = block; j < block + threadLoad; j++)
					{
						//find the selected json
						Bson filter = Filters.eq(j);
						//there is always just one document with a specific ID
						Document document = streamTable.find(filter).first();
						if (document == null)
						{
							System.out.println("Document is null");
							continue;
						}

						//create a queue containing the json 
						Queue<Character> q = new LinkedList<>();
						String json = (String) document.get("json");
						if (json == null || json.equals(""))
						{
							System.out.println("json is empty");
							continue;
						}
						
						for (int k = 0; k < json.length(); k++)
						{
							q.add(json.charAt(k));
						}

						//create a map representation of json
						HashMap<String, Object> map = stringToMap(q);
						//find useful information
						HashMap<String, Object> info = null;
						try
						{
							info = findTokens(map);
						} catch (Exception ex)
						{
							System.out.println("searching for tokens failed!");
						}
						//save to database
						saveToDB(info);
					}
				}
			});
			//executor.execute(thread);
		}
		executor.shutdown();
		executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);  //never ever
		mongoClient.close();
		System.out.println("http requests count : " + HTTPREQ.get());
	}

	/**
	 * Saves information to database
	 * @param map 
	 */
	public static void saveToDB(HashMap<String, Object> map)
	{
		//find selected fields
		Long id = (Long) map.get("user_id");
		Long timestamp = (Long) map.get("timestamp");
		boolean isRetweet = (Boolean) map.get("is_retweet");
		Long retweet = null;
		if (isRetweet)
		{
			retweet = (Long) map.get("retweet_id");
		}
		HashSet<String> hashtags = (HashSet<String>) map.get("hashtags");
		HashSet<String> mentions = (HashSet<String>) map.get("mentions");
		HashMap<String, String> urls = (HashMap<String, String>) map.get("urls");
		
		//save above fields 
		Document document;
		Bson filter = Filters.eq(id);
		Bson update;
		FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
		options.upsert(true);
		options.returnDocument(ReturnDocument.AFTER);

		if (isRetweet)
		{
			document = new Document();
			document.put("_id", retweet);
			retweetTable.insertOne(document);

			update = new BasicDBObject("retweets", new BasicDBObject("id", retweet).append("time", timestamp));
			update = new BasicDBObject("$push", update);
			userTable.findOneAndUpdate(filter, update, options);
		}

		for (String hashtag : hashtags)
		{
			document = new Document();
			document.put("id", hashtag);
			hashtagTable.insertOne(document);

			update = new BasicDBObject("hashtags", new BasicDBObject("hashtag", hashtag).append("time", timestamp));
			update = new BasicDBObject("$push", update);
			userTable.findOneAndUpdate(filter, update, options);
		}

		for (String mention : mentions)
		{
			document = new Document();
			document.put("id", mention);
			mentionTable.insertOne(document);

			update = new BasicDBObject("mentions", new BasicDBObject("mention", mention).append("time", timestamp));
			update = new BasicDBObject("$push", update);
			userTable.findOneAndUpdate(filter, update, options);
		}

		for (Entry<String, String> entry : urls.entrySet())
		{
			document = new Document();
			document.put("id", entry.getKey());
			document.put("tco", entry.getValue());
			urlTable.insertOne(document);

			update = new BasicDBObject("urls", new BasicDBObject("url", entry.getKey()).append("time", timestamp));
			update = new BasicDBObject("$push", update);
			userTable.findOneAndUpdate(filter, update, options);
		}
	}

	/**
	 * Finds useful information from a map representing a json object
	 * @param map a map containing a json object
	 * @return a map containing only useful information
	 * @throws Exception 
	 */
	public static HashMap<String, Object> findTokens(HashMap<String, Object> map) throws Exception
	{
		HashMap<String, Object> result = new HashMap<>();
		//find user id
		HashMap<String, Object> user = (HashMap<String, Object>) map.get("user");
		if (user == null)
		{
			System.out.println("User table is null");
			return null;
		}
		String id_str = (String) user.get("id_str");
		if (id_str == null || id_str.equals(""))
		{
			System.out.println("ID String is null/empty");
			return null;
		}
		Long id = Long.valueOf(id_str);
		result.put("user_id", id);

		//find timestamp
		String time_str = (String) map.get("timestamp_ms");
		if (time_str == null || time_str.equals(""))
		{
			System.out.println("Timestamp is null/empty");
			return null;
		}
		Long timestamp = Long.valueOf(time_str);
		result.put("timestamp", timestamp);

		HashSet<String> hashtags = new HashSet<>();
		HashSet<String> mentions = new HashSet<>();
		HashMap<String, String> urls = new HashMap<>();
		result.put("hashtags", hashtags);
		result.put("mentions", mentions);
		result.put("urls", urls);

		//find retweet id, if there is any
		Boolean rq = null;
		Long retweet_id;
		HashMap<String, Object> tmp = (HashMap<String, Object>) map.get("retweeted_status");
		if (tmp != null)
		{
			rq = true;
		} else
		{
			tmp = (HashMap<String, Object>) map.get("quoted_status");
			if (tmp != null)
			{
				rq = false;
			}
		}

		if (tmp != null)
		{
			result.put("is_retweet", true);
			retweet_id = Long.valueOf((String) tmp.get("id_str"));
			result.put("retweet_id", retweet_id);
			if (rq)
			{
				//retwets do not contain additional information
				return result;
			}
		} else
		{
			result.put("is_retweet", false);
		}

		//find text
		String text = (String) map.get("text");
		if (text == null)
		{
			System.out.println("Text is null");
			return null;
		}

		//find hashtags
		Pattern hashtagPattern = Pattern.compile("#([a-zA-Z]\\w*)");
		Matcher hashtagMatcher = hashtagPattern.matcher(text);
		while (hashtagMatcher.find())
		{
			String s = hashtagMatcher.group(1).toLowerCase();
			hashtags.add(s);
		}

		//find mentions
		Pattern mentionPattern = Pattern.compile("@(\\w*)");
		Matcher mentionMatcher = mentionPattern.matcher(text);
		while (mentionMatcher.find())
		{
			String s = mentionMatcher.group(1).toLowerCase();
			mentions.add(s);
		}

		//find urls
		String lastAdded = null;
		Pattern urlPattern = Pattern.compile("(https://t.co/.*?(\\s|$))");
		Matcher urlMatcher = urlPattern.matcher(text);
		while (urlMatcher.find())
		{
			String shortUrl = urlMatcher.group(1);
			String temp;
			//expand url recursivly till null is returned
			//null means the url we expanded was not a tiny url
			String expand = expandURL(shortUrl);
			int max = 0;
			if (expand == null)
			{
				break;
			}
			//max url attemps
			while (max < 5)
			{
				temp = expand;
				expand = expandURL(expand);
				max++;
				if (expand == null)
				{
					if (temp.startsWith("http"))
					{
						lastAdded = temp;
						urls.put(temp, shortUrl);
					}
					break;
				}
			}
		}
		//delete last url if tweet was a quote.
		//every quote's last url is a url pointing to the original tweet
		if (rq != null)
		{
			if (rq == false)
			{
				urls.remove(lastAdded);
			}
		}

		return result;
	}

	/**
	 * Executes an HTTP request
	 * @param str a url
	 * @return the location of the header field from the html returned
	 * @throws Exception
	 */
	public static String expandURL(String str) throws Exception
	{
		HTTPREQ.getAndIncrement();
		URL url;
		try
		{
			url = new URL(str);
		} catch (Exception e)
		{
			return null;
		}
		// open connection
		HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
		// follow browser redirect
		httpURLConnection.setInstanceFollowRedirects(false);
		// timeout after 5 seconds
		httpURLConnection.setConnectTimeout(30 * 1000);
		// extract location header containing the actual destination URL
		String expandedURL = httpURLConnection.getHeaderField("Location");
		httpURLConnection.disconnect();
		return expandedURL;
	}

	/**
	 * Reads a map element from the queue
	 * @param q
	 * @return 
	 */
	public static HashMap<String, Object> stringToMap(Queue<Character> q)
	{
		HashMap<String, Object> map = new HashMap<>();

		pollCheck(q, OPENCURLYBRACKET);
		do
		{
			if (peekCheck(q, CLOSECURLYBRACKET))
			{
				q.poll();
				checkForComma(q);
				return map;
			}
			//read attribute name
			String key = readString(q);
			pollCheck(q, COLON);
			//attribute value is number
			if (numberFormat(q))
			{
				String number = readNumber(q);
				if (isLong(number))
				{
					long value = Long.parseLong(number);
					map.put(key, value);
				} else
				{
					double value = Double.parseDouble(number);
					map.put(key, value);
				}
			}
			//attribute value is string
			if (peekCheck(q, DOUBLEQUOTATIONMARK))
			{
				String value = readString(q);
				map.put(key, value);
			}
			//attribute value is true,false or null
			if (Character.isLetter(q.peek()))
			{
				if (peekCheck(q, 'n'))
				{
					map.put(key, null);
				}
				if (peekCheck(q, 't'))
				{
					map.put(key, true);
				}
				if (peekCheck(q, 'f'))
				{
					map.put(key, false);
					q.poll();
				}
				for (int i = 0; i < 4; i++)
				{
					q.poll();
				}
			}
			//attribute value is another object
			if (peekCheck(q, OPENCURLYBRACKET))
			{
				map.put(key, stringToMap(q));
			}
			//attribute value is a list
			if (peekCheck(q, OPENBRACKET))
			{
				map.put(key, stringToArray(q));
			}
			checkForComma(q);
			//continue
		} while (!q.isEmpty());
		//return should be invoked through polling the last closing curly bracket, thus this return point is invalid
		return null;
	}

	/**
	 * Reads an array of element from a queue
	 * @param q
	 * @return 
	 */
	private static ArrayList<Object> stringToArray(Queue<Character> q)
	{
		ArrayList<Object> list = new ArrayList<>();

		pollCheck(q, OPENBRACKET);
		do
		{
			boolean commaSeperated = true;
			if (peekCheck(q, CLOSEBRACKET))
			{
				q.poll();
				checkForComma(q);
				return list;
			}
			//read an object
			if (peekCheck(q, OPENCURLYBRACKET))
			{
				commaSeperated = false;
				list.add(stringToMap(q));
			}
			//read another list
			if (peekCheck(q, OPENBRACKET))
			{
				commaSeperated = false;
				list.add(stringToArray(q));
			}
			//if none of the above fit, array contains comma separated elements
			if (commaSeperated)
			{
				StringBuilder sb = new StringBuilder();
				while (!peekCheck(q, CLOSEBRACKET))
				{
					sb.append(q.poll());
				}
				String cs = sb.toString();
				list.addAll(Arrays.asList(cs.split(",")));
			}
			checkForComma(q);
		} while (!q.isEmpty());
		//return should be invoked through polling the last closing curly bracket, thus this return point is invalid
		return null;
	}

	/**
	 * Removes the next character of the queue, if it is a comma
	 * @param q 
	 */
	private static void checkForComma(Queue<Character> q)
	{
		if (peekCheck(q, COMMA))
		{
			q.poll();
		}
	}

	/**
	 * Reads a string from a queue
	 * Strings are boxed inside double quotation marks
	 * @param q
	 * @return 
	 */
	private static String readString(Queue<Character> q)
	{
		pollCheck(q, DOUBLEQUOTATIONMARK);
		StringBuilder sb = new StringBuilder();
		boolean fakeQuotes = false;
		while (!peekCheck(q, DOUBLEQUOTATIONMARK) || fakeQuotes)
		{
			if (peekCheck(q, BACKSLASH))
			{
				fakeQuotes = !fakeQuotes;
			} else
			{
				fakeQuotes = false;
			}
			sb.append(q.poll());

		}
		pollCheck(q, DOUBLEQUOTATIONMARK);
		return sb.toString();
	}

	
	/**
	 * Checks if the next character of the queue matches a specific character
	 *  by removing the first character
	 * @param q 
	 * @param c 
	 * @return 
	 */
	private static void pollCheck(Queue<Character> q, Character c)
	{
		if (!q.poll().equals(c))
		{
			System.out.println("(  " + c + " ) not found");
		}
	}

	/**
	 * Checks if the next character of the queue matches a specific character
	 * without modifying the queue
	 * @param q 
	 * @param c 
	 * @return 
	 */
	private static boolean peekCheck(Queue<Character> q, Character c)
	{
		if (!q.isEmpty())
		{
			return q.peek().equals(c);
		}
		return false;
	}

	/**
	 * Checks whether the next character in the queue is part of a number
	 * @param q
	 * @return 
	 */
	private static boolean numberFormat(Queue<Character> q)
	{
		Character c = q.peek();
		//infinity, NaN are excluded
		return (Character.isDigit(c) || c.equals(MINUS) || c.equals(PLUS) || c.equals(DOT));
	}

	/**
	 * Reads a number from the queue
	 * @param q a queue
	 * @return the number read from the queue
	 */
	private static String readNumber(Queue<Character> q)
	{
		StringBuilder sb = new StringBuilder();
		while (numberFormat(q))
		{
			sb.append(q.poll());
		}
		return sb.toString();
	}
	
	/**
	 * checks whether the input is a Long number or not
	 * @param str the input to be checked
	 * @return true if the input is a Long number else false
	 */
	private static boolean isLong(String str)
	{
		try
		{
			Long.parseLong(str);
			return true;
		} catch (NumberFormatException e)
		{
			return false;
		}
	}
}
