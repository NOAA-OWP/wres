import java.util.*;

/**
 * Implement this UNIX/LINUX 
 * sort -t, -k1d,1 -k2,2 -k3,3 -k6n,6 -k7n,7 -k4,4 -k5,5 -k8,8 -k9n,9
 * command with Comparator interface
 * Created: December, 2018
 *
 * @see ThePairs class
 * @author <a href="mailto:Raymond.Chui@***REMOVED***">
 * @version 1.5
 */
public class SortedPairs implements Comparator<ThePairs> {

	// Override the Comparator.compare for Collections.sort in multiple ArrayList
	@Override
    public int compare(ThePairs tp1, ThePairs tp2) {

		int c;
		c = tp1.getLeadDurationPairInSeconds().intValue() - tp2.getLeadDurationPairInSeconds().intValue(); // column 9 in numeric 
		if ( c == 0)
			c = tp1.getLatestLeadTimeInSeconds().intValue() - tp2.getLatestLeadTimeInSeconds().intValue(); // column 7 in numeric 
			if (c == 0)
				c = tp1.getEarlestLeadTimeInSeconds().intValue() - tp2.getEarlestLeadTimeInSeconds().intValue(); // column 6 in numeric 
				if (c == 0)
					 c = tp1.getSiteID().compareTo(tp2.getSiteID()); // column 1 in dictiorary
		return c;
	} // end this method
} // end this class
