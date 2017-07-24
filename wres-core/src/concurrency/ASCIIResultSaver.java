package concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.io.concurrency.WRESRunnable;
import wres.io.utilities.Database;
import wres.util.Collections;

import java.sql.SQLException;
import java.util.HashMap;

/**
 * @author ctubbs
 *
 */
@Deprecated
public class ASCIIResultSaver extends WRESRunnable {

	private static final Logger LOGGER = LoggerFactory.getLogger(ASCIIResultSaver.class);
	
	public ASCIIResultSaver(HashMap<Integer, HashMap<String, String[]>> forecasted_values, Integer observationlocation_id)
	{
		this.forecasted_values = forecasted_values;
		this.observationlocation_id = observationlocation_id;
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void execute() {
		try {
			save_mapping();
		} 
		catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private void save_mapping() throws SQLException
	{
		boolean add_comma = false;

		for (int forecast : forecasted_values.keySet())
		{
			for (String hour : forecasted_values.get(forecast).keySet())
			{
				if (add_comma)
				{
					expression_builder.append(", ");
				}
				else
				{
					add_comma = true;
				}
				
				String values = Collections.toString(forecasted_values.get(forecast).get(hour));
				expression_builder.append("(");
				expression_builder.append(forecast);
				expression_builder.append(", ");
				expression_builder.append(hour);
				expression_builder.append(", '{");
				expression_builder.append(values);
				expression_builder.append("}', ");
				expression_builder.append(observationlocation_id);
				expression_builder.append(")");
			}
		}
		
		expression_builder.append(";");		
		Database.execute(expression_builder.toString());

	}
	
	private final HashMap<Integer, HashMap<String, String[]>> forecasted_values;
	private final Integer observationlocation_id;
	private final StringBuilder expression_builder = new StringBuilder("INSERT INTO ForecastResult(forecast_id, lead_time, measurements, observationlocation_id) VALUES ");

	@Override
	protected String getTaskName () {
		return "ASCIIResultSaver-" + String.valueOf(Thread.currentThread().getId());
	}

	@Override
	protected Logger getLogger () {
		return ASCIIResultSaver.LOGGER;
	}
}
