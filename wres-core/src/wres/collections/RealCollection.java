/**
 * 
 */
package wres.collections;

import java.util.Collection;
import java.util.List;
import java.util.Vector;
import java.util.function.BiFunction;
import java.util.function.DoubleFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;


/**
 * @author ctubbs
 *
 */
public class RealCollection extends Vector<Double> {

	/**
	 * 
	 */
	public double mean()
	{
		return sum()/elementCount;
	}
	
	public double sum()
	{
		double summation = 0.0;
		for (int i = 0; i < elementCount; ++i)
		{
			summation += get(i);
		}
		return summation;
	}
	
	public double sum_range(double minimum, double maximum)
	{
		RealCollection sorted_values = where((Double value) -> {
			return value >= minimum && value <= maximum;
		});
		
		return sorted_values.sum();
	}
	
	public double sigma(DoubleFunction<Double> expression)
	{
		double summation = 0.0;
		for (int i = 0; i < elementCount; ++i)
		{
			summation = expression.apply(get(i));
		}
		return summation;
	}
	
	public RealCollection copy()
	{
		RealCollection copied_collection = new RealCollection();
		parallelStream().collect(Collectors.toCollection(()->copied_collection));
		return copied_collection;
	}
	
	public RealCollection where(Predicate<? super Double> expression)
	{
		RealCollection copy = new RealCollection();
		parallelStream().filter(expression).collect(Collectors.toCollection(()->copy));
		copy.sort(null);
		return copy;
	}
	
	public double sigma(Function<Double, Double> expression)
	{
		double summation = 0.0;
		for (int i = 0; i < elementCount; ++i)
		{
			summation = expression.apply(get(i));
		}
		return summation;
	}
	
	public double sigma(BiFunction<Double, List<Double>, Double> expression)
	{
		double summation = 0.0;
		for (int i = 0; i < elementCount; ++i)
		{
			summation += expression.apply(get(i), copy());
		}
		
		return summation;
	}
	
	public double median()
	{
		double middle = 0.0;
		
		if (elementCount > 0)
		{
			int middle_position = (int)elementCount / 2;
			middle = get(middle_position);
			
			if (elementCount % 2 == 0)
			{
				middle += get(middle_position - 1);
				middle = middle / 2;
			}
		}
		
		return middle;
	}
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7436292764130685565L;

	/**
	 * 
	 */
	public RealCollection() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param initialCapacity
	 */
	public RealCollection(int initialCapacity) {
		super(initialCapacity);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param c
	 */
	public RealCollection(Collection<? extends Double> c) {
		super(c);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param initialCapacity
	 * @param capacityIncrement
	 */
	public RealCollection(int initialCapacity, int capacityIncrement) {
		super(initialCapacity, capacityIncrement);
		// TODO Auto-generated constructor stub
	}

}
