/**
 * 
 */
package config.data;

import collections.FourTuple;

/**
 * @author ctubbs
 *
 */
public class Aggregation extends FourTuple<Boolean, Byte, String, String> {

	/**
	 * 
	 */
	public Aggregation(Boolean aggregate, Byte range, String unit, String mode) 
	{
		super(aggregate, range, unit, mode);
	}

	public Boolean get_aggregate()
	{
		return get_item_one();
	}
	
	public Byte get_range()
	{
		return get_item_two();
	}
	
	public String get_unit()
	{
		return get_item_three();
	}
	
	public String get_mode()
	{
		return get_item_four();
	}
}
