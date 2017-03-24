/**
 * 
 */
package wres.concurrency;
import wres.util.Database;

import java.sql.SQLException;
import java.util.List;

/**
 * @author ctubbs
 *
 */
public class AsyncBatchExecutor implements Runnable {

	/**
	 * 
	 */
	public AsyncBatchExecutor(String prepared_script, List<List<Object>> parameters) {
		this.prepared_script = prepared_script;
		this.parameters = parameters;
	}

	@Override
	public void run() 
	{
		try 
		{
			Database.batch_execute_prepared(prepared_script, parameters);
		} catch (SQLException e) 
		{
			e.printStackTrace();
		}		
	}
	
	private String prepared_script;
	private List<List<Object>> parameters;
}