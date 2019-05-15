package twitterproject;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * A map keeping the k-best elements. 
 * @author achilles
 * @param <K> 
 * @param <V>
 */
public class MyTreeMap<K extends Comparable, V>
{

	private final TreeMap<K, ArrayList<V>> map;
	private final int capacity;
	private int currentSize;

	/**
	 * Constructor
	 * @param size maximum capacity of the map 
	 */
	public MyTreeMap(int size)
	{
		this.capacity = size;
		this.currentSize = 0;
		this.map = new TreeMap<>();

	}

	/**
	 * adds an entry to the map
	 * @param key key
	 * @param value value
	 */
	public void put(K key, V value)
	{
		if (this.currentSize < this.capacity)
		{
			if (!this.map.containsKey(key))
			{
				ArrayList<V> v = new ArrayList<>();
				this.map.put(key, v);
			}
			this.map.get(key).add(value);
			this.currentSize++;
		} else if (this.map.firstKey().compareTo(key) < 0)
		{
			this.pollFirstElement();
			this.put(key, value);
		}
	}

	/**
	 * Removes and returns the first element of the map
	 * @return the first element of the map
	 */
	public Entry<K, V> pollFirstElement()
	{
		if (this.map.isEmpty())
		{
			return null;
		}
		K key = this.map.firstKey();
		List<V> list = this.map.get(key);
		V value = list.remove(0);
		if (list.isEmpty())
		{
			this.map.pollFirstEntry();
		}
		this.currentSize--;
		return new AbstractMap.SimpleEntry<>(key, value);
	}
	
	/**
	 * Returns a the values stored in this map
	 * @return a collections containing all values.
	 */
	public Collection<ArrayList<V>> values()
	{
		return this.map.values();
	}
	
	public int size()
	{
		return this.currentSize;
	}

	@Override
	public String toString()
	{
		return this.map.toString();
	}

}
