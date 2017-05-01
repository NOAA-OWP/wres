/**
 * 
 */
package concurrency;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A collection of Metrics that may be performed on selected data
 * 
 * @author Christopher Tubbs
 */
public final class Metrics {
	
	@Deprecated
	/**
	 * Creates a function that will transform and evaluate the mean error of all observed and forecasted values
	 * from the database
	 * 
	 * The result set MUST contain a column containing an array of floats named "measurements" and a column containing a float
	 * named "measurement"
	 * 
	 * @return The mean error of all values in the result set
	 * @Deprecated Relies on the old schema which expects forecasted values to be contained within an array
	 */
	public static BiFunction<ResultSet, Function<Double, Double>, Double> calculateMeanError()
	{
		return (ResultSet dataset, Function<Double, Double> scale_func) -> {
			Double sum = 0.0;
			int value_count = 0;
			Float[] ensemble_values;
			Double observation;
			
			try {
				while (dataset.next())
				{
					ensemble_values = (Float[])dataset.getArray("measurements").getArray();
					observation = dataset.getFloat("measurement")*1.0;
					
					for (int ensemble_index = 0; ensemble_index < ensemble_values.length; ++ensemble_index)
					{
						value_count++;		
						sum += scale_func.apply(ensemble_values[ensemble_index]*1.0) - scale_func.apply(observation);
					}
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
			return sum/value_count;
		};
	}

	/**
	 * Creates the "meanError" function to run on selected data
	 * @return The function to perform mean error calculations on a set of data containing the columns
	 * "forecast" and "observation".
	 */
	public static BiFunction<ResultSet, Function<Double, Double>, Double> meanError() {
	    return (ResultSet dataset, Function<Double, Double> scaleFunc) -> {
	        Double sum = 0.0;
	        Double intermediateSum = 0.0;
	        int valueCount = 0;
	        
	        try {
	            while (dataset.next()) {
	                intermediateSum = (dataset.getFloat("forecast") * 1.0) - (dataset.getFloat("observation") * 1.0);
	                sum += scaleFunc.apply(intermediateSum);
	                valueCount++;
	            }
	        } catch (SQLException error) {
	            error.printStackTrace();
	        }
	        
	        
	        
	        return sum / valueCount;
	    };
	}
}
