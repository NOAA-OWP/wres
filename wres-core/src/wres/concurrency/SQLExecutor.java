/**
 * 
 */
package wres.concurrency;

import java.sql.ResultSet;
import java.util.concurrent.Callable;

import wres.util.Database;

/**
 * @author ctubbs
 *
 */
public class SQLExecutor implements Callable<ResultSet> {

	/**
	 * 
	 */
	public SQLExecutor(String script)
	{
		this.script = script;
	}
	
	private String script;

	@Override
	public ResultSet call() throws Exception {
		// TODO Auto-generated method stub
		return Database.execute_for_result(script);
	}

}
