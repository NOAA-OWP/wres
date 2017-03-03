/**
 * 
 */
package wres.reading.misc;

import java.sql.SQLException;
import java.util.HashMap;

import com.google.common.base.Stopwatch;

/**
 * @author ctubbs
 *
 */
public class ASCIIEntryParser implements Runnable {
	
	public ASCIIEntryParser(HashMap<Integer, HashMap<String, String[]>> forecasted_values)
	{
		this.forecasted_values = forecasted_values;
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			save_mapping();
		} 
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void save_mapping() throws SQLException
	{
		//Connection connection = null;
		//try {

			boolean add_comma = false;
			int insert_count = 0;

			for (int forecast : forecasted_values.keySet())
			{
				for (String hour : forecasted_values.get(forecast).keySet())
				{
	
					for (String value : forecasted_values.get(forecast).get(hour))
					{
						if (add_comma)
						{
							expression_builder.append(", ");
						}
						else
						{
							add_comma = true;
						}
						
						expression_builder.append("(");
						expression_builder.append(forecast);
						expression_builder.append(", ");
						expression_builder.append(hour);
						expression_builder.append(", ");
						expression_builder.append(value);
						expression_builder.append(")");
						insert_count++;
					}
				}
			}

			expression_builder.append(";");
			//wres.util.Utilities.add_query(expression_builder.toString());

			//System.err.println("Now inserting " + String.valueOf(insert_count) + " values for " + forecasted_values.size() + " forecasts...");
			wres.util.Utilities.execute_eds_query(expression_builder.toString());
		/*} catch (SQLException error) {
			System.err.println("The following query could not be executed:");
			System.err.println();
			System.err.println(expression_builder.toString());
			throw error;
		}*/

	}
	
	private HashMap<Integer, HashMap<String, String[]>> forecasted_values;
	private StringBuilder expression_builder = new StringBuilder("INSERT INTO ForecastResult(forecast_id, lead_time, measurement) VALUES ");;
}
