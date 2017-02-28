/**
 * 
 */
package wres.util;

/**
 * @author ctubbs
 *
 */
public enum TemporalUnit {
	UNDEFINED,
	YEAR,
	MONTH,
	WEEK,
	DAY,
	HOUR,
	MINUTE,
	SECOND;

	public TemporalUnit value_by_name(String name)
	{
		name = name.toUpperCase();
		return TemporalUnit.valueOf(name);
	}
}
