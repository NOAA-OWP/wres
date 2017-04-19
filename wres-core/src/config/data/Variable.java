/**
 * 
 */
package config.data;

import collections.TwoTuple;

/**
 * @author ctubbs
 *
 */
public class Variable extends TwoTuple<String, String> {

	/**
	 * @param name
	 * @param unit
	 */
	public Variable(String name, String unit) {
		super(name, unit);
	}

	public String getName()
	{
		return this.itemOne();
	}
	
	public String getUnit()
	{
		return this.itemTwo();
	}
}
