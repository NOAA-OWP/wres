package wres.datamodel.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A factory class for manipulating metadata.
 * 
 * @author james.brown@hydrosolved.com
 */

public final class MetadataFactory
{

    /**
     * Finds the union of the input, based on the {@link TimeWindow}. All components of the input must be equal, 
     * except the {@link TimeWindow}, otherwise an exception is thrown. See also {@link TimeWindow#unionOf(List)}.
     * 
     * @param input the input metadata
     * @return the union of the input
     * @throws MetadataException if the input is invalid
     */

    public static Metadata unionOf( List<Metadata> input )
    {
        String nulLString = "Cannot find the union of null metadata.";
        if ( Objects.isNull( input ) )
        {
            throw new MetadataException( nulLString );
        }
        if ( input.isEmpty() )
        {
            throw new MetadataException( "Cannot find the union of empty input." );
        }
        List<TimeWindow> unionWindow = new ArrayList<>();
        Metadata test = input.get( 0 );
        for ( Metadata next : input )
        {
            if ( Objects.isNull( next ) )
            {
                throw new MetadataException( nulLString );
            }
            if ( !next.equalsWithoutTimeWindow( test ) )
            {
                throw new MetadataException( "Only the time window can differ when finding the union of metadata." );
            }
            if ( next.hasTimeWindow() )
            {
                unionWindow.add( next.getTimeWindow() );
            }
        }
        if ( !unionWindow.isEmpty() )
        {
            test = Metadata.of( test, TimeWindow.unionOf( unionWindow ) );
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
