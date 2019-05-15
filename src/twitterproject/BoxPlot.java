package twitterproject;

import java.util.List;
import java.util.Map.Entry;

/**
 * An iconic representation of a boxplot.
 * @author achilles
 */
public class BoxPlot<V>
{
	private final double q1;
	private final double q2;
	private final double q3;
	private final double iqr;
	private final double lowerBound;
	private final double upperBound;

	public BoxPlot(List<Entry<Integer, V>> list)
	{
		//find medians
		int size = list.size();
		int split = size / 2;
		if (isEven(size))
		{
			this.q2 = medianEven(list, 0, size);
			if (isEven(split))
			{
				this.q1 = medianEven(list, 0, split);
				this.q3 = medianEven(list, split, size);
			} else
			{
				this.q1 = medianOdd(list, 0, split);
				this.q3 = medianOdd(list, split, size);
			}

		} else
		{
			this.q2 = medianOdd(list, 0, size);
			if (isEven(split))
			{
				this.q1 = medianEven(list, 0, split);
				this.q3 = medianEven(list, split + 1, size);
			} else
			{
				this.q1 = medianOdd(list, 0, split);
				this.q3 = medianOdd(list, split + 1, size);
			}
		}
		//define bounds
		iqr = q3 - q1;
		lowerBound = q1 - iqr * 1.5;
		upperBound = q3 + iqr * 1.5;
	}
	
	/**
	 * Checks if an item exceeds the upper bound
	 * @param example the item to be checked
	 * @return true if the item exceeds the upper bound, else false
	 */
	public boolean isOutlierUpper(Integer example)
	{
		return example > upperBound ;
	}
	
	/**
	 * Checks if an item exceeds the lower bound
	 * @param example the item to be checked
	 * @return true if the item exceeds the lower bound, else false
	 */
	public boolean isOutlierLower(Integer example)
	{
		return example < lowerBound ;
	}

	/**
	 * Finds the median of a sublist. It's size must be an even number
	 * @param list the list containing the sublist
	 * @param from index of the first element of sublist
	 * @param to index of the last element of sublist
	 * @return the median of the sublist
	 */
	private double medianEven(List<Entry<Integer, V>> list, int from, int to)
	{
		int size = to - from;
		int t1 = (size / 2) - 1;
		int t2 = t1 + 1;
		Integer n1 = list.get(from + t1).getKey();
		Integer n2 = list.get(from + t2).getKey();
		return (n1 + n2) / 2.0;
	}

	/**
	 * Finds the median of a sublist. It's size must be an odd number
	 * @param list the list containing the sublist
	 * @param from index of the first element of sublist
	 * @param to index of the last element of sublist
	 * @return the median of the sublist
	 */
	private double medianOdd(List<Entry<Integer, V>> list, int from, int to)
	{
		int size = to - from;
		int t1 = size / 2;
		return list.get(from + t1).getKey();
	}

	/**
	 * Checks if the input is even
	 * @param num the number to be checked
	 * @return true if the input is even, else false
	 */
	private boolean isEven(int num)
	{
		return num % 2 == 0;
	}

}
