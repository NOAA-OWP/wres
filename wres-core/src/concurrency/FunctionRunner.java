/**
 * 
 */
package concurrency;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Function;

import config.SystemConfig;
import util.Database;

/**
 * A callable thread that will perform a passed in function on a set of values that will be returned from the database
 */
public class FunctionRunner<V, U> implements Callable<V> {

	/**
	 * Creates a thread with the given query to select data and a function to call on the data selected
	 * 
	 * @param data_select The query used to select data
	 * @param func The function to call on all selected data. It must accept a SQL ResultSet and a Function to modify data/return the data within
	 */
	public FunctionRunner(String data_select, BiFunction<ResultSet, Function<U, U>, V> func)
	{
		this.data_select = data_select;
		this.func = func;
		this.transform = (U value) -> {
			return value;
		};
	}
	
	/**
	 * Creates the thread with the given query to select data, a function to call on the selected data, and a function used to transform
	 * retrieved data prior to manipulation
	 * 
	 * @param data_select The query used to select data from the database
	 * @param func A function to call on all selected values. It must accept a SQL ResultSet and a Function to modify data/return the data within
	 * @param transform A function receiving the indicated type and returning a modified version of the passed in value
	 */
	public FunctionRunner(String data_select, BiFunction<ResultSet, Function<U, U>, V> func, Function<U, U> transform) {
		this.data_select = data_select;
		this.func = func;
		if (transform == null)
		{
			this.transform = (U value) -> {
				return value;
			};
		}
		else
		{
			this.transform = transform;
		}
	}

	@Override
	/**
	 * Executes the functions on the passed in result set
	 * 
	 * A ResultSet is created by selecting data based on the dictated query, then the function is called
	 * with the retrieved values and either the default transform (just returns the unaltered value) or the
	 * passed in transform.
	 */
	public V call() throws Exception {
		V function_result = null;
		Connection connection = null;
		try
		{
			connection = Database.get_connection();
			Statement query = connection.createStatement();
			query.setFetchSize(SystemConfig.instance().get_fetch_size());
			ResultSet result = query.executeQuery(data_select);
			function_result = func.apply(result, transform);	
		}
		catch (Exception error)
		{
			if (connection != null)
			{
				connection.rollback();
			}
		}
		finally
		{
			if (connection != null)
			{
				Database.return_connection(connection);
			}
		}
		return function_result;
	}
	
	private String data_select;
	private BiFunction<ResultSet, Function<U, U>, V> func;
	private Function<U, U> transform;

}
