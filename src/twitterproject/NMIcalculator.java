package twitterproject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Calculates the NMI metric between graphs.
 *
 * @author achilles
 */
public class NMIcalculator
{

	//paths to graphs
	private static final List<Entry<String, String>> PATHS = new ArrayList<>();

	//insert your locations before executing
	static
	{
		PATHS.add(new AbstractMap.SimpleEntry<>("hashtag", "gephi/hashtag/hashtag modularity classes.csv"));
		PATHS.add(new AbstractMap.SimpleEntry<>("mention", "gephi/mention/mention modularity classes.csv"));
		PATHS.add(new AbstractMap.SimpleEntry<>("url", "gephi/url/url modularity classes.csv"));
		PATHS.add(new AbstractMap.SimpleEntry<>("retweet", "gephi/retweet/retweet modularity classes.csv"));
		PATHS.add(new AbstractMap.SimpleEntry<>("total", "gephi/total/total modularity classes.csv"));
	}

	public static void main(String[] args) throws IOException
	{
		//communities list. Each element contains the name of the graph, followed by a map representing the graph itself
		//a graph is saved as entries of cluster- nodes 
		List<Entry<String, Map<Integer, Set<Long>>>> com = new ArrayList<>();
		for (int i = 0; i < PATHS.size(); i++)
		{
			Map<Integer, Set<Long>> map = new HashMap<>();
			//open file
			try (BufferedReader reader = new BufferedReader(new FileReader(PATHS.get(i).getValue())))
			{
				//skip first line, which contains  csv headers
				reader.readLine();
				while (reader.ready())
				{
					String line = reader.readLine();
					String[] token = line.split(",");
					Long id = Long.valueOf(token[0]);
					Integer cluster = Integer.valueOf(token[1]);
					Set<Long> set = map.get(cluster);
					if (set == null)
					{
						set = new HashSet<>();
						set.add(id);
						map.put(cluster, set);
					} else
					{
						set.add(id);
					}
				}
				com.add(new AbstractMap.SimpleEntry<>(PATHS.get(i).getKey(), map));
			}
		}

		Iterator<Entry<Integer, Set<Long>>> iteratorA;
		Iterator<Entry<Integer, Set<Long>>> iteratorB;
		//for each combination between graphs
		for (int i = 0; i < com.size(); i++)
		{
			String a = com.get(i).getKey();
			Map<Integer, Set<Long>> mapA = com.get(i).getValue();

			for (int j = i + 1; j < com.size(); j++)
			{
				String b = com.get(j).getKey();
				Map<Integer, Set<Long>> mapB = com.get(j).getValue();
				int n = n(mapA, mapB);

				//calculate numerator of NMI
				double numerator = 0;
				iteratorA = mapA.entrySet().iterator();
				while (iteratorA.hasNext())
				{
					Entry<Integer, Set<Long>> entryA = iteratorA.next();
					int clusterID_A = entryA.getKey();
					int ni = entryA.getValue().size();

					iteratorB = mapB.entrySet().iterator();
					while (iteratorB.hasNext())
					{
						Entry<Integer, Set<Long>> entryB = iteratorB.next();
						int clusterID_B = entryB.getKey();

						int nj = entryB.getValue().size();
						int nij = n(mapA, clusterID_A, mapB, clusterID_B);

						double temp = nij * log((nij * n) / (ni * nj));
						numerator += temp;
					}
				}
				numerator *= 2;

				//claculate denominator
				double denominator = 0;
				iteratorA = mapA.entrySet().iterator();
				while (iteratorA.hasNext())
				{
					Entry<Integer, Set<Long>> entryA = iteratorA.next();
					int ni = entryA.getValue().size();

					double temp = ni * log(ni / n);
					denominator += temp;
				}

				iteratorB = mapB.entrySet().iterator();
				while (iteratorB.hasNext())
				{
					Entry<Integer, Set<Long>> entryB = iteratorB.next();
					int nj = entryB.getValue().size();

					double temp = nj * log(nj / n);
					denominator += temp;
				}
				double result = Math.abs(numerator / denominator);

				System.out.println(a + " - " + b + "  : " + result);
			}
		}

	}

	private static int log(int x)
	{
		return (int) (Math.log(x) / Math.log(2));
	}

	/**
	 * calculates Nij
	 *
	 * @param a i's map
	 * @param c1 i's cluster index
	 * @param b j's map
	 * @param c2 j's cluster index
	 * @return Nij
	 */
	private static int n(Map<Integer, Set<Long>> a, Integer c1, Map<Integer, Set<Long>> b, Integer c2)
	{
		Set<Long> s1 = a.get(c1);
		Set<Long> s2 = b.get(c2);
		if (s1 == null || s2 == null)
		{
			return 0;
		}
		Set<Long> intersection = new HashSet<>(s1);
		intersection.retainAll(s2);
		return intersection.size();
	}

	/**
	 * Counts the number of unique nodes in both graphs
	 *
	 * @param a i's graph
	 * @param b j's graph
	 * @return N
	 */
	private static int n(Map<Integer, Set<Long>> a, Map<Integer, Set<Long>> b)
	{
		Set<Long> nodes = new HashSet<>();

		for (Set<Long> set : a.values())
		{
			nodes.addAll(set);
		}
		for (Set<Long> set : b.values())
		{
			nodes.addAll(set);
		}

		return nodes.size();
	}

}
