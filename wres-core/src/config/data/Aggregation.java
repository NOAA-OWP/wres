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
		return getItemOne();
	}
	
	public Byte get_range()
	{
		return getItemTwo();
	}
	
	public String get_unit()
	{
		return getItemThree();
	}
	
	public String get_mode()
	{
		return getItemFour();
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
