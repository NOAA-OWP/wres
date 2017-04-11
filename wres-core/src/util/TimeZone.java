/**
 * 
 */
package util;

/**
 * @author ctubbs
 *
 */
public enum TimeZone {
	UNDEFINED (-99.0),
	UTC (0.0),
	EGT (-1.0),
	GST (-2.0),
	NDT (-2.5),
	ADT (-3.0),
	NST (-3.5),
	EDT (-4.0),
	EST (-5.0),
	CST (-6.0),
	MST (-7.0),
	PST (-8.0),
	AKST (-9.0),
	MIT (-9.5),
	HAST (-10.0),
	SST (-11.0),
	BIT (-12.0);	
	
	public final double offset;
	TimeZone(double offset)
	{
		this.offset = offset;
	}

	public TimeZone value_by_name(String name)
	{
		name = name.toUpperCase();
		return TimeZone.valueOf(name);
	}
}
