package twitterproject;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.Document;

/**
 * Finds similarity tables from users stored in database
 *
 * @author achilles
 */
public class FindSimilarityTable
{

	private static MongoClient mongoClient;
	private static MongoDatabase db;
	private static MongoCollection<Document> similarity;
	private static MongoCollection<Document> userTable;

	public static void main(String[] args) throws InterruptedException
	{
		//connect to database
		MongoClientOptions clientOptions = MongoClientOptions
				.builder()
				.writeConcern(WriteConcern.UNACKNOWLEDGED)
				.build();
		mongoClient = new MongoClient("127.0.0.1", clientOptions);
		db = mongoClient.getDatabase("Twitter");

		similarity = db.getCollection("similarity");
		userTable = db.getCollection("userTable");

		//initializing similarity gives us access to outliers
		Similarity s = new Similarity();

		//find and filter users
		ArrayList<UserDocument> users = new ArrayList<>();
		Document sort = new Document("_id", 1);
		//for every user, we find entities and filter them so outliers are removed
		for (Document user : userTable.find().sort(sort))
		{
			UserDocument ud = new UserDocument((long) user.get("_id"));
			ArrayList<Document> userList = (ArrayList<Document>) user.get("urls");
			if (!(userList == null))
			{
				List<String> list = s.transformList(userList, "url");
				list.removeAll(s.getOutlierUrls());
				ud.getUrlMap().addAll(list);
			}
			userList = (ArrayList<Document>) user.get("mentions");
			if (!(userList == null))
			{
				List<String> list = s.transformList(userList, "mention");
				list.removeAll(s.getOutlierMentions());
				ud.getMentionMap().addAll(list);
			}
			userList = (ArrayList<Document>) user.get("hashtags");
			if (!(userList == null))
			{
				List<String> list = s.transformList(userList, "hashtag");
				list.removeAll(s.getOutlierHashtags());
				ud.getHashtagMap().addAll(list);
			}
			userList = (ArrayList<Document>) user.get("retweets");
			if (!(userList == null))
			{
				List<String> list = s.transformList(userList, "id");
				list.removeAll(s.getOutlierRetweets());
				ud.getRetweetMap().addAll(list);
			}
			ud.calcAccumulators();
			//if there are entities present, we save that user to our list
			if (!ud.isEmpty())
			{
				users.add(ud);
			}
		}

		Iterator<UserDocument> iterator = users.iterator();
		while (iterator.hasNext())
		{
			UserDocument doc = iterator.next();
			if (doc.numOfEntities() < 5)
			{
				iterator.remove();
			}
		}

		//outliers are no longed needed, so we free some space
		s.setOutlierMentions(null);
		s.setOutlierHashtags(null);
		s.setOutlierUrls(null);
		s.setOutlierRetweets(null);

		//calculate similarity tables.
		//we split the workload to a number of threads
		int mapSize = 100000;
		ArrayList<Future<MyTreeMap<Double, Document>>> results = new ArrayList<>();
		AtomicInteger atomicInteger = new AtomicInteger(users.size() - 1);
		int numOfThreads = 4;
		ExecutorService executor = Executors.newFixedThreadPool(numOfThreads);
		for (int i = 0; i < numOfThreads; i++)
		{
			Callable<MyTreeMap<Double, Document>> callable = new Callable<MyTreeMap<Double, Document>>()
			{
				@Override
				public MyTreeMap<Double, Document> call() throws Exception
				{
					int counter;
					//every thread has its own tree map
					MyTreeMap<Double, Document> treeMap = new MyTreeMap<>(mapSize);
					//while there are users for whom we have not calculated similarities
					while ((counter = atomicInteger.getAndDecrement()) > 0)
					{
						UserDocument user1 = users.get(counter);
						//j counter shows which users should be compared with the selected user
						for (int j = counter - 1; j >= 0; j--)
						{
							UserDocument user2 = users.get(j);
							s.findSimilarities(user1, user2, treeMap);
						}
						if (counter % 100 == 0)
						{
							System.out.println(counter);
						}
					}
					return treeMap;
				}
			};
			results.add(executor.submit(callable));
		}
		executor.shutdown();
		//wait for threads to finish execution
		executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

		//save results to database
		for (int i = 0; i < results.size(); i++)
		{
			Future<MyTreeMap<Double, Document>> obj = results.get(i);
			if (obj.isDone())
			{
				try
				{
					MyTreeMap<Double, Document> map = obj.get();
					for (ArrayList<Document> list : map.values())
					{
						similarity.insertMany(list);
					}
				} catch (ExecutionException ex)
				{
					System.out.println("error with " + i + " map");
				}
			}

		}
		mongoClient.close();
	}

}
