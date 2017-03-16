/**
 * 
 */
package wres.concurrency;

import java.sql.ResultSet;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Function;

import wres.util.Database;

/**
 * @author ctubbs
 *
 */
public class FunctionRunner<V, U> implements Callable<V> {

	/**
	 * 
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
	public V call() throws Exception {
		ResultSet result = Database.execute_for_result(data_select);
		return func.apply(result, transform);
	}
	
	private String data_select;
	private BiFunction<ResultSet, Function<U, U>, V> func;
	private Function<U, U> transform;

}
