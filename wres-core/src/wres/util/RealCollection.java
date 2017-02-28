/**
 * 
 */
package wres.util;

import java.util.Collection;
import java.util.List;
import java.util.Vector;
import java.util.function.DoubleFunction;
import wres.util.delegation.*;

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
	
	public double sigma(DoubleFunction<Double> expression)
	{
		double summation = 0.0;
		for (int i = 0; i < elementCount; ++i)
		{
			summation = expression.apply(get(i));
		}
		return summation;
	}
	
	public double sigma(ArgFunction<Double, Double> expression)
	{
		double summation = 0.0;
		for (int i = 0; i < elementCount; ++i)
		{
			summation = expression.apply(get(i));
		}
		return summation;
	}
	
	public double sigma(TwoArgFunction<Double, List<Double>, Double> expression)
	{
		double summation = 0.0;
		for (int i = 0; i < elementCount; ++i)
		{
			summation = expression.apply(get(i), this.subList(0, elementCount - 1));
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
