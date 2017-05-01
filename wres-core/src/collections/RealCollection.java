/**
 * 
 */
package collections;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.DoubleFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;


/**
 * A collection of real numbers
 * 
 * @author Christopher Tubbs
 *
 */
public class RealCollection extends ArrayList<Double> {

	/**
	 * Returns the simple mean of the numbers in the collection
	 */
	public Double mean() {
		Double mean = null;
		if (this.size() > 0) {
			mean = sum()/this.size();
		}
		return mean;
	}
	
	/**
	 * Returns the sum of all values in the collection
	 */
	public double sum() {
		double summation = 0.0;
		for (int i = 0; i < this.size(); ++i) {
			summation += get(i);
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
	public double sum_range(double minimum, double maximum) {
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
		for (int i = 0; i < this.size(); ++i) {
			summation = expression.apply(get(i));
		}
		return summation;
	}
	
	/**
	 * Returns a new RealCollection with identical values
	 * @return a new, identical RealCollection
	 */
	public RealCollection copy() {
		RealCollection copied_collection = new RealCollection();
		stream().collect(Collectors.toCollection(()->copied_collection));
		return copied_collection;
	}
	
	/**
	 * Selects all values deemed acceptable by the passed in boolean function
	 * @param expression A function accepting a Double as a parameter and returning a boolean value
	 * @return A subset of the collection adhering to the passed in function
	 */
	public RealCollection where(Predicate<? super Double> expression) {
		RealCollection copy = new RealCollection();
		stream().filter(expression).collect(Collectors.toCollection(()->copy));
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
		for (int i = 0; i < this.size(); ++i) {
			summation = expression.apply(get(i));
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
		
		for (int i = 0; i < this.size(); ++i) {
			summation += expression.apply(get(i), copiedCollection);
		}
		
		return summation;
	}
	
	public double median() {
		double middle = 0.0;
		
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
		for (int index = 0; index < values.length; ++index)
		{
			this.set(index, values[index]);
		}
	}

}
