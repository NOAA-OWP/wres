/**
 * 
 */
package wres.util;

/**
 * @author ctubbs
 *
 */
public enum Statistic {
	UNKNOWN,
	ACCUMULATED,
	INSTANTANEOUS,
	MEAN,
	TOTAL,
	MAXIMUM,
	MINIMUM,
	MEDIAN,
	MODE,
	FIRST,
	LAST;
	
	public Statistic value_by_name(String name)
	{
		name = name.toUpperCase();
		return Statistic.valueOf(name);
	}
}
