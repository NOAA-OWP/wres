/**
 * 
 */
package wres.reading.misc;

import java.sql.SQLException;
import java.util.HashMap;

/**
 * @author ctubbs
 *
 */
public class ASCIIEntryParser implements Runnable {
	
	public ASCIIEntryParser(int variable_id, HashMap<String, String[]> hourly_values)
	{
		this.forecast_id = String.valueOf(variable_id);
		this.hourly_values = hourly_values;
		expression_builder = new StringBuilder("INSERT INTO ForecastResult(forecast_id, lead_time, measurement) VALUES ");
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

			for (String key : hourly_values.keySet())
			{

				for (String value : hourly_values.get(key))
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
					expression_builder.append(forecast_id);
					expression_builder.append(", ");
					expression_builder.append(key);
					expression_builder.append(", ");
					expression_builder.append(value);
					expression_builder.append(")");
				}
			}

			expression_builder.append(";");
			//wres.util.Utilities.add_query(expression_builder.toString());
			wres.util.Utilities.execute_eds_query_async(expression_builder.toString());
			
		/*} catch (SQLException error) {
			System.err.println("The following query could not be executed:");
			System.err.println();
			System.err.println(expression_builder.toString());
			throw error;
		}*/

	}
	
	private String forecast_id;
	private HashMap<String, String[]> hourly_values;
	private StringBuilder expression_builder;
}
