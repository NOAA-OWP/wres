package concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.io.concurrency.WRESRunnable;
import wres.io.utilities.Database;
import wres.util.Time;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.HashMap;

/**
 * @author ctubbs
 *
 */
public class DatacardResultSaver extends WRESRunnable {

	private static final Logger LOGGER = LoggerFactory.getLogger(DatacardResultSaver.class);

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
	public void execute() {
		try {
			save();
		} catch (SQLException e) {
			e.printStackTrace();
		}
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
			this.getLogger().error("The following query could not be executed:");
			this.getLogger().error("");
			this.getLogger().error(expression.toString());
			throw error;
		}
	}

	private final String observation_id;
	private final HashMap<OffsetDateTime, String> dated_values;
	private final StringBuilder expression;

	@Override
	protected String getTaskName () {
		return "DatacardResultSaver-" + String.valueOf(Thread.currentThread().getId());
	}

	@Override
	protected Logger getLogger () {
		return DatacardResultSaver.LOGGER;
	}
}
