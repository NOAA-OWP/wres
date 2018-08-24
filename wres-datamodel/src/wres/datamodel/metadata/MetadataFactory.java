package wres.datamodel.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import wres.datamodel.thresholds.OneOrTwoThresholds;

/**
 * A factory class for manipulating metadata.
 * 
 * @author james.brown@hydrosolved.com
 */

public final class MetadataFactory
{

    /**
     * Finds the union of the input, based on the {@link TimeWindow}. All components of the input must be equal, 
     * except the {@link SampleMetadata#getTimeWindow()} and {@link SampleMetadata#getThresholds()}, otherwise an exception is 
     * thrown. See also {@link TimeWindow#unionOf(List)}. No threshold information is represented in the union.
     * 
     * @param input the input metadata
     * @return the union of the input
     * @throws MetadataException if the input is invalid
     */

    public static SampleMetadata unionOf( List<SampleMetadata> input )
    {
        String nullString = "Cannot find the union of null metadata.";
        if ( Objects.isNull( input ) )
        {
            throw new MetadataException( nullString );
        }
        if ( input.isEmpty() )
        {
            throw new MetadataException( "Cannot find the union of empty input." );
        }
        List<TimeWindow> unionWindow = new ArrayList<>();
        
        // Test entry
        SampleMetadata test = input.get( 0 );

        // Validate for equivalence with the first entry and add window to list
        for ( SampleMetadata next : input )
        {
            if ( Objects.isNull( next ) )
            {
                throw new MetadataException( nullString );
            }
            if ( !next.equalsWithoutTimeWindowOrThresholds( test ) )
            {
                throw new MetadataException( "Only the time window and thresholds can differ when finding the union of "
                        + "metadata." );
            }
            if ( next.hasTimeWindow() )
            {
                unionWindow.add( next.getTimeWindow() );
            }
        }
        
        // Remove any threshold information from the result
        test = SampleMetadata.of( test, (OneOrTwoThresholds) null );

        if ( !unionWindow.isEmpty() )
        {
            test = SampleMetadata.of( test, TimeWindow.unionOf( unionWindow ) );
        }
        return test;
    }

    /**
     * No argument constructor.
     */

    private MetadataFactory()
    {
    }

}
