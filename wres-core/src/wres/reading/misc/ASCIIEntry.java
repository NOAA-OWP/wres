/**
 * 
 */
package wres.reading.misc;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import wres.reading.BasicSeriesEntry;

/**
 * @author ctubbs
 *
 */
public class ASCIIEntry extends BasicSeriesEntry {

	/**
	 * 
	 */
	public ASCIIEntry() {
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public String toString()
	{
		DateFormat formatter = new SimpleDateFormat("MM/dd/yyyy HH");
		String message = formatter.format(date);
		message += " ";
		message += String.valueOf(lead_time);
		
		for (Double value : values)
		{
			message += " ";
			message += String.valueOf(value);
		}
		
		return message;
	}
}
