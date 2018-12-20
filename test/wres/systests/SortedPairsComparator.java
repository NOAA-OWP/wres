package wres.systests;

import java.util.Comparator;

/**
 * Implement this UNIX/LINUX 
 * sort -t, -k1d,1 -k2,2 -k3,3 -k6n,6 -k7n,7 -k4,4 -k5,5 -k8,8 -k9n,9
 * command with Comparator interface
 * Created: December, 2018
 *
 * @see PairsFilePair class
 * @author <a href="mailto:Raymond.Chui@***REMOVED***">
 * @version 1.5
 */
public class SortedPairsComparator implements Comparator<PairsFilePair>
{
    //TODO I am not sure that this sort works correctly.  It only looks at four fields when it should be looking at 9 fields,
    //as shown in the example sort command above.  Or is that command still valid???  Check it out, Hank!!!

    // Override the Comparator.compare for Collections.sort in multiple ArrayList
    @Override
    public int compare( PairsFilePair tp1, PairsFilePair tp2 )
    {

        int c;
        c = tp1.getLeadDurationPairInSeconds().intValue() - tp2.getLeadDurationPairInSeconds().intValue(); // column 9 in numeric 
        if ( c == 0 )
            c = tp1.getLatestLeadTimeInSeconds().intValue() - tp2.getLatestLeadTimeInSeconds().intValue(); // column 7 in numeric 
        if ( c == 0 )
            c = tp1.getEarlestLeadTimeInSeconds().intValue() - tp2.getEarlestLeadTimeInSeconds().intValue(); // column 6 in numeric 
        if ( c == 0 )
            c = tp1.getSiteID().compareTo( tp2.getSiteID() ); // column 1 in dictiorary
        return c;
    }
} 
