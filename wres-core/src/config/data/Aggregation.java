/**
 * 
 */
package config.data;

/**
 * Specification for how to aggregate values in a metric
 * @author Christopher Tubbs
 */
public class Aggregation {

	/**
	 * Constructor
	 * @param aggregate Whether or not to aggregate values
	 * @param range The span of the following unit to aggregate
	 * @param unit The unit with which to aggregate over
	 * @param mode Identifier for the method of aggregation
	 */
	public Aggregation(Boolean aggregate, Byte range, String unit, String mode) 
	{
	    this.performAggregation = aggregate;
	    this.aggregationRange = range;
	    this.aggregationUnit = unit;
	    this.aggregationMode = mode;
	}

	/**
	 * @return Whether or not to perform an aggregation
	 */
	public Boolean performAggregation()
	{
		return this.performAggregation;
	}
	
	/**
	 * @return The range of aggregation
	 */
	public Byte aggregationRange()
	{
		return this.aggregationRange;
	}
	
	/**
	 * @return The unit of the range for the aggregation
	 */
	public String aggregationUnit()
	{
		return this.aggregationUnit;
	}
	
	/**
	 * @return The method to use for aggregation
	 */
	public String aggregationMode()
	{
		return this.aggregationMode;
	}
	
	@Override
	public String toString() {
		String description = "Aggregation: ";
		description += System.lineSeparator();
		
		description += "\tAggregate Data: ";
		description += String.valueOf(performAggregation());
		description += System.lineSeparator();
		
		description += "\tRange: ";
		description += String.valueOf(aggregationMode());
		description += " ";
		description += String.valueOf(aggregationUnit());
		description += System.lineSeparator();
		
		description += "\tAggregation Mode: ";
		description += String.valueOf(aggregationMode());
		description += System.lineSeparator();
		
		
		return description;
	}
	
	private final boolean performAggregation;
	private final byte aggregationRange;
	private final String aggregationUnit;
	private final String aggregationMode;
}
