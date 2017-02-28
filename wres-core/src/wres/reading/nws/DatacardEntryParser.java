/**
 * 
 */
package wres.reading.nws;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author ctubbs
 *
 */
public class DatacardEntryParser implements Runnable {

	/**
	 * 
	 */
	public DatacardEntryParser(int variable_id,
							   OffsetDateTime date,
							   String value) 
	{
		this.variable_id = variable_id;
		format_date(date);
		this.value = value;
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		// TODO Auto-generated method stub
		//utils.Utilities.execute_eds_query(String.format(save_observation, variable_id, date, value));
		try {
			save();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void save() throws SQLException
	{
		Connection connection = null;
		String command = String.format(save_observation, variable_id, date, value);
		try {
			connection = wres.util.Utilities.create_eds_connection();
			Statement query = connection.createStatement();
			query.execute(command);
		} catch (SQLException error) {
			System.err.println("The following query could not be executed:");
			System.err.println();
			System.err.println(command);
			throw error;
		}
		finally
		{
			if (connection != null)
			{
				connection.close();
			}
		}
	}
	
	private void format_date(OffsetDateTime datetime)
	{
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		date = datetime.format(formatter);
	}

	private int variable_id;
	private String date;
	private String value;
	private static String save_observation = "INSERT INTO ObservationResult (observation_id, valid_date, measurement) " +
			"VALUES (%d, '%s', %s);";
}
