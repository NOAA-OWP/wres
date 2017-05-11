/**
 * 
 */
package collections;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.DoubleFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import static java.util.stream.Collectors.*;

import wres.datamodel.VectorOfDoubles;


/**
 * A collection of real numbers
 * 
 * @author Christopher Tubbs
 *
 */
public class RealCollection extends LinkedList<Double> implements Comparable<RealCollection>, VectorOfDoubles
{
    /**
     * Adds a float to the collection as a double
     * @param value The Float to add
     */
    public void add(Float value) 
    {
        this.add(value * 1.0);
    }
    
    /**
     * Adds an unboxed float to the collection as a double
     * @param value The float to add
     */
    public void add(float value)
    {
        this.add(value * 1.0);
    }
    
    /**
     * Adds an Integer to the collection as a double
     * @param value The Integer to add
     */
    public void add(Integer value)
    {
        this.add(value * 1.0);
    }
    
    /**
     * Adds an unboxed int to the collection as a double
     * @param value The int to add
     */
    public void add(int value)
    {
        this.add(value * 1.0);
    }
    
    /**
     * Adds an unboxed short to the collection as a double
     * @param value The short to add
     */
    public void add(short value)
    {
        this.add(value * 1.0);
    }
    
    /**
     * Adds a Short to the collection as a double
     * @param value The Short to add
     */
    public void add(Short value)
    {
        this.add(value * 1.0);
    }
    
    /**
     * Adds a Long to the collection as a double
     * @param value The Long to add
     */
    public void add(Long value)
    {
        this.add(value * 1.0);
    }
    
    /**
     * Adds an unboxed long to the collection as a double
     * @param value The long to add
     */
    public void add(long value)
    {
        this.add(value * 1.0);
    }
    
	/**
	 * @return the simple mean of the numbers in the collection
	 */
	public Double mean() {
		Double mean = null;
		
		if (this.size() > 0) {
			mean = sum()/this.size();
		}
		
		return mean;
	}
	
	/**
	 * @return the sum of all values in the collection
	 */
	public double sum() {
		double summation = 0.0;
		
		for (final Double value : this)
		{
		    summation += value;
		}
		
		return summation;
	}
	
	/**
	 * Adds all numbers contained within the collection between the minimum and maximum
	 * 
	 * @param minimum The smallest acceptable number to add
	 * @param maximum The largest acceptable number to add
	 * @return The sum of all numbers between the minimum and maximum, inclusive
	 */
	public double sumRange(double minimum, double maximum) {
		RealCollection sorted_values = where((Double value) -> {
			return value >= minimum && value <= maximum;
		});
		
		return sorted_values.sum();
	}
	
	/**
	 * Adds all of the resulting values from the passed in function
	 * @param expression A mathematical function taking a Double and returning a Double
	 * @return The summation of all results from the function
	 */
	public double sigma(DoubleFunction<Double> expression) {
		double summation = 0.0;
		for (final Double value : this)
		{
            summation = expression.apply(value);
		}

		return summation;
	}
	
	/**
	 * @return a new RealCollection with identical values
	 */
	public RealCollection copy() {
		RealCollection copied_collection = new RealCollection();
		stream().collect(toCollection(()->copied_collection));
		return copied_collection;
	}
	
	/**
	 * Selects all values deemed acceptable by the passed in boolean function
	 * @param expression A function accepting a Double as a parameter and returning a boolean value
	 * @return A subset of the collection adhering to the passed in function
	 */
	public RealCollection where(Predicate<? super Double> expression) {
		RealCollection copy = new RealCollection();
		stream().filter(expression).collect(toCollection(()->copy));
		copy.sort(null);
		return copy;
	}
	
	/**
	 * Adds all of the resulting values from the passed in function
	 * @param expression A mathematical function taking a Double and returning a Double
	 * @return The summation of all results from the function
	 */
	public double sigma(Function<Double, Double> expression) {
		double summation = 0.0;
		
		for (final Double value : this)
		{
            summation = expression.apply(value); 
		}
		
		return summation;
	}
	
	/**
	 * Performs a summation by passing in each value with a copy of the collection
	 * @param expression The mathematical function to perform
	 * @return The result 
	 */
	public double sigma(BiFunction<Double, List<Double>, Double> expression) {
		double summation = 0.0;
		final RealCollection copiedCollection = this.copy();
		
		for (final Double value : this)
		{
            summation += expression.apply(value, copiedCollection);
		}
		
		return summation;
	}
	
	/**
	 * @return The value in the middle of the collection
	 */
	public Double median() {
		Double middle = null;
		
		if (this.size() > 0) {
			int middle_position = this.size() / 2;
			middle = get(middle_position);
			
			if (this.size() % 2 == 0) {
				middle += get(middle_position - 1);
				middle = middle / 2;
			}
		}
		
		return middle;
	}
	
	/**
	 * Returns a list containing every value within the given percentile
	 * @param percent The percentile of the collection to obtain
	 * @return A subset of the collection within the given percentile
	 */
	public List<Double> getPercentileValues(int percent)
	{
	    List<Double> values = this.copy();
	    Collections.sort(values);
	    return values.subList(0, getPercentilePosition(percent));
	}
	
	/**
	 * The position of the percentile within the collection
	 * @param percent The percentile rank
	 * @return The position of the percentile rank within the current collection
	 */
	private int getPercentilePosition(int percent) 
	{
	    int position = -1;
	    
	    if (percent >= 100) {
	        position = this.size();
	    } else {
	        position = Math.min(this.size(), (percent / 100) * this.size());
	    }  
	    
	    return position;
	}
	
	/**
	 * Returns the value situated at the given percentile
	 * @param percent The percentile rank
	 * @return The value for the percentile
	 */
	public Double getPercentileValue(int percent)
	{
	    Double percentile = null;
	    int position = getPercentilePosition(percent);
	    
	    if (position < this.size())
	    {
	        percentile = this.get(position);
	    }
	    
	    return percentile;
	}
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7436292764130685565L;

	/**
	 * Creates an empty collection of real numbers
	 */
	public RealCollection() {}
	
	/**
	 * Creates a collection containing the passed in values
	 * @param values The values to insert into the collection
	 */
	public RealCollection(double[] values)
	{
        for (double value : values)
        {
            this.add(value);
        }
	}

    public RealCollection(Float[] values)
    {
        for (Float value : values)
        {
            this.add(value);
        }
    }
    
    public RealCollection(float[] values)
    {
        for (float value : values)
        {
            this.add(value);
        }
    }
    
    public RealCollection(Double[] values)
    {
        for (Double value : values)
        {
            this.add(value);
        }
    }
    
    public RealCollection(int[] values)
    {
        for (int value : values)
        {
            this.add(value);
        }
    }
    
    public RealCollection(Integer[] values)
    {
        for (Integer value : values)
        {
            this.add(value);
        }
    }
    
    public RealCollection(short[] values)
    {
        for (short value : values)
        {
            this.add(value);
        }
    }
    
    public RealCollection(Long[] values)
    {
        for (Long value : values)
        {
            this.add(value);
        }
    }
    
    public RealCollection(long[] values)
    {
        for (long value : values)
        {
            this.add(value);
        }
    }
    
    public RealCollection(Double value)
    {
        this.add(value);
    }
    
    public RealCollection(double value)
    {
        this.add(value);
    }
    
    public RealCollection(float value)
    {
        this.add(value);
    }
    
    public RealCollection(Float value)
    {
        this.add(value);
    }
    
    public RealCollection(Integer value)
    {
        this.add(value);
    }
    
    public RealCollection(int value)
    {
        this.add(value);
    }
    
    public RealCollection(short value)
    {
        this.add(value);
    }
    
    public RealCollection(Short value)
    {
        this.add(value);
    }
    
    public RealCollection(long value)
    {
        this.add(value);
    }
    
    public RealCollection(Long value)
    {
        this.add(value);
    }

    @Override
    public int compareTo(RealCollection other)
    {
        int comparison = 0;  
        if (other == null || this.size() > other.size())
        {
            comparison = 1;
        }
        else if (this.size() == 0 && other.size() == 0)
        {
            comparison = 0;
        }
        else if (this.size() < other.size())
        {
            comparison = -1;
        }
        else
        {
            Double mean = this.mean();
            Double otherMean = other.mean();
            
            if (mean > otherMean)
            {
                comparison = 1;
            }
            else if (mean < otherMean)
            {
                comparison = -1;
            }
            else
            {
                Double median = this.median();
                Double otherMedian = other.median();
                
                if (median > otherMedian)
                {
                    comparison = 1;
                }
                else if (median < otherMedian)
                {
                    comparison = -1;
                }
            }
        }
        
        return comparison;
    }

    @Override
    public double[] getDoubles()
    {
        return this.stream().mapToDouble(d -> d).toArray();
    }
}
