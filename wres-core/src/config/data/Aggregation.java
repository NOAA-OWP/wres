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
	
	@Override
	public String toString() {
		String description = "Aggregation: ";
		description += System.lineSeparator();
		
		description += "\tAggregate Data: ";
		description += String.valueOf(get_aggregate());
		description += System.lineSeparator();
		
		description += "\tRange: ";
		description += String.valueOf(get_range());
		description += " ";
		description += String.valueOf(get_unit());
		description += System.lineSeparator();
		
		description += "\tAggregation Mode: ";
		description += get_mode();
		description += System.lineSeparator();
		
		
		return description;
	}
}
