package twitterproject;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

/**
 * A map which counts the unique elements that it is containing
 *
 * @author achilles
 * @param <K> the class this map will be using. K must implement hashcode and
 * equals.
 */
public class CountMap<K>
{

	private final HashMap<K, Integer> map;

	/**
	 * Constructor
	 */
	public CountMap()
	{
		//initialize the map
		this.map = new HashMap<>();
	}

	/**
	 * Constructor
	 *
	 * @param collection the collection to be added
	 */
	public CountMap(Collection<K> collection)
	{
		this();
		this.addAll(collection);
	}

	/**
	 * Removes elements based on their value
	 *
	 * @param value the value to be deleted
	 * @return a set containing all the removed elements
	 */
	public Set<K> removeValues(Integer value)
	{
		HashSet<K> result = new HashSet<>();
		Iterator<Entry<K, Integer>> it = this.map.entrySet().iterator();
		while (it.hasNext())
		{
			Entry<K, Integer> entry = it.next();
			Integer x = entry.getValue();
			if (x.equals(value))
			{
				result.add(entry.getKey());
				it.remove();
			}
		}
		return result;
	}

	/**
	 * Clears the map. A clean map contains no elements
	 */
	public void clearAll()
	{
		this.map.clear();
	}

	/**
	 * Returns the times an element was added to this map
	 *
	 * @param key the element to be checked
	 * @return the times the element was added
	 */
	public Integer get(K key)
	{
		Integer value;
		if ((value = this.map.get(key)) != null)
		{
			return value;
		} else
		{
			return 0;
		}
	}

	/**
	 * Adds an element to the map
	 *
	 * @param obj the element to be added
	 */
	public void add(K obj)
	{
		Integer value;
		if ((value = this.map.get(obj)) != null)
		{
			this.map.replace(obj, value + 1);
		} else
		{
			this.map.put(obj, 1);
		}
	}

	/**
	 * Adds a collection to the map
	 *
	 * @param collection the collection to be added
	 */
	public final void addAll(Collection<K> collection)
	{
		Iterator<K> it = collection.iterator();
		while (it.hasNext())
		{
			K obj = it.next();
			this.add(obj);
		}

	}

	/**
	 * Sorts the elements of the map
	 *
	 * @return a list containing the sorted elements
	 */
	public ArrayList<Entry<Integer, K>> getValuesSorted()
	{
		ArrayList<Entry<Integer, K>> list = new ArrayList<>();
		for (Entry<K, Integer> entry : this.map.entrySet())
		{
			list.add(new SimpleEntry<>(entry.getValue(), entry.getKey()));
		}
		Collections.sort(list, new EntryComparator<>());
		return list;
	}

	/**
	 * Calculates the formula : Square-root(Sum-all (value squared) )
	 *
	 * @return the result of the formula
	 */
	public double accumulate()
	{
		int sum = 0;
		for (Entry<K, Integer> entry : this.map.entrySet())
		{
			int value = entry.getValue();
			sum += (value * value);
		}
		return Math.sqrt(sum);
	}

	/**
	 * Calculates the dot product between this and a given countmap
	 *
	 * @param m a countmap
	 * @return the dot product of the countmaps
	 */
	public int dotProduct(CountMap<K> m)
	{
		int result = 0;
		for (Entry<K, Integer> entry : this.map.entrySet())
		{
			K key = entry.getKey();
			Integer value = entry.getValue();
			result += value * m.get(key);
		}
		return result;
	}

	public int size()
	{
		return this.map.size();
	}

	public int numOfItems()
	{
		int counter = 0;
		for (Integer entry : this.map.values())
		{
			counter += entry;
		}
		return counter;
	}
}
