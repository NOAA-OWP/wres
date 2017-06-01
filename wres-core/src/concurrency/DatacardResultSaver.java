package concurrency;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.HashMap;
import wres.io.utilities.Database;
import wres.util.Time;
import wres.io.concurrency.WRESTask;

/**
 * @author ctubbs
 *
 */
public class DatacardResultSaver extends WRESTask implements Runnable {

	/**
	 * 
	 */
	public DatacardResultSaver(String observation_id,
							   HashMap<OffsetDateTime, String> dated_values) 
	{
		this.observation_id = observation_id;
		this.dated_values = dated_values;
		this.expression = new StringBuilder("INSERT INTO ObservationResult (observation_id, valid_date, measurement, observationlocation_id) VALUES ");
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
	    this.executeOnRun();
		try {
			save();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		this.exectureOnComplete();
	}
	
	private void save() throws SQLException
	{
		try {
			boolean first_value = true;
			
			for (OffsetDateTime valid_time : dated_values.keySet())
			{
				if (!first_value)
				{
					expression.append(", ");
				}
				else
				{
					first_value = false; 
				}
				
				expression.append("(");
				expression.append(observation_id);
				expression.append(", '");
				expression.append(Time.convertDateToString(valid_time));
				expression.append("', ");
				expression.append(dated_values.get(valid_time));
				expression.append(", 1)");
			}
			
			expression.append(";");
			
			Database.execute(expression.toString());
		} catch (SQLException error) {
			System.err.println("The following query could not be executed:");
			System.err.println();
			System.err.println(expression.toString());
			throw error;
		}
	}

	private String observation_id;
	private HashMap<OffsetDateTime, String> dated_values;
	private StringBuilder expression;
}
