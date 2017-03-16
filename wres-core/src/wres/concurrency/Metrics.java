/**
 * 
 */
package wres.concurrency;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author ctubbs
 *
 */
public class Metrics {
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			return sum/value_count;
		};
	}

}
