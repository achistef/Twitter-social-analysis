package twitterproject;

import java.util.Comparator;
import java.util.Map.Entry;

/**
 * Compares entries. Key must be an Integer
 * @author achilles
 */
public class EntryComparator<V> implements Comparator<Entry<Integer, V>>
{

	public EntryComparator()
	{
		//empty constructor
	}

	@Override
	public int compare(Entry<Integer, V> o1, Entry<Integer, V> o2)
	{
		Integer a = o1.getKey();
		Integer b = o2.getKey();
		return a.compareTo(b);
	}
}
