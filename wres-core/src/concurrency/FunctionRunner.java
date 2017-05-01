/**
 * 
 */
package concurrency;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Function;

import util.Database;

/**
 * A callable thread that will perform a passed in function on a set of values that will be returned from the database
 * 
 * @author Christopher Tubbs
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
		this.dataSelect = data_select;
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
		this.dataSelect = data_select;
		this.func = func;
		
		// If there is no transform, set it to the identity function
		if (transform == null) {
			transform = (U value) -> {
				return value;
			};
		}

        this.transform = transform;
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
		V functionResult = null;
		Connection connection = null;
		
		try {
			connection = Database.getConnection();
			ResultSet result = Database.getResults(connection, dataSelect);
			functionResult = func.apply(result, transform);	
		} finally {
			Database.returnConnection(connection);
		}
		return functionResult;
	}
	
	private String dataSelect;
	private BiFunction<ResultSet, Function<U, U>, V> func;
	private Function<U, U> transform;

}
