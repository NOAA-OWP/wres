/**
 * 
 */
package concurrency;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import config.specification.MetricSpecification;
import config.specification.ScriptBuilder;
import util.Database;
import wres.datamodel.DataFactory;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;

/**
 * A collection of Metrics that may be performed on selected data
 * 
 * @author Christopher Tubbs
 */
public final class Metrics {
    
    /**
     * The Metrics class is a collection of functions; it should not be instantiated
     */
    private Metrics() {}
	
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
	
	public static List<PairOfDoubleAndVectorOfDoubles> getPairs(MetricSpecification metricSpecification, int progress) throws Exception {
        List<PairOfDoubleAndVectorOfDoubles> pairs = new ArrayList<>();
        Connection connection = null;

        try
        {
            connection = Database.getConnection();
            String script = ScriptBuilder.generateGetPairData(metricSpecification, progress);            
            
            ResultSet resultingPairs = Database.getResults(connection, script);
            
            while (resultingPairs.next())
            {
                Double observedValue = resultingPairs.getDouble("sourceOneValue");
                Double[] forecasts = (Double[]) resultingPairs.getArray("measurements").getArray();
                pairs.add(DataFactory.pairOf(observedValue, forecasts));
            }
        }
        catch (Exception error)
        {
            System.err.println("Pairs could not be retrieved for the following parameters:");
            System.err.println("Progress: " + progress);
            System.err.println();
            System.err.println("The metric was:");
            System.err.println(metricSpecification.toString());
            System.err.println();
            error.printStackTrace();
            System.err.println();
            throw error;
        }
        finally
        {
            if (connection != null)
            {
                Database.returnConnection(connection);
            }
        }
        return pairs;
	}
}
