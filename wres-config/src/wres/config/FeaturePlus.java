package wres.config;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.SortedMap;

import wres.config.generated.CoordinateSelection;
import wres.config.generated.Feature;

/**
 * Class that wraps a {@link Feature} and implements {@link Comparable} on the contents of the {@link Feature}. As such,
 * {@link FeaturePlus} may be used as keys in a {@link SortedMap} or in any other context for which natural order must 
 * be preserved.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 1.0
 */

public class FeaturePlus implements Comparable<FeaturePlus>
{

    /**
     * The wrapped {@link Feature}.
     */

    private final Feature feature;

    /**
     * Returns an instance of a {@link FeaturePlus}.
     * 
     * @param feature the {@link Feature} to wrap
     * @return a {@link FeaturePlus}
     * @throws NullPointerException of the input is null
     */

    public static FeaturePlus of( Feature feature )
    {
        return new FeaturePlus( feature );
    }
    
    /**
     * Compares two {@link FeaturePlus} based on the {@link Feature#getLocationId()} of their contained features only.
     * 
     * @param left the left feature
     * @param right the right feature
     * @return a negative integer, zero, or a positive integer as the left feature is less than, equal to, or greater 
     *            than the right feature, based on location identifier only
     */

    public static int compareByLocationId( FeaturePlus left, FeaturePlus right )
    {
        Objects.requireNonNull( left, "Specify non-null left feature for comparison." );
        
        Objects.requireNonNull( right, "Specify non-null right feature for comparison." );
        
        return left.getFeature().getLocationId().compareTo( right.getFeature().getLocationId() );        
    }
    
    /**
     * Returns the wrapped {@link Feature}.
     * 
     * @return the wrapped {@link Feature}
     */

    public Feature getFeature()
    {
        return feature;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof FeaturePlus ) )
        {
            return false;
        }
        // Compare contents
        Feature in = ( (FeaturePlus) o ).getFeature();
        boolean first = Objects.equals( in.getAlias(), feature.getAlias() )
                        && compareCoordinates( in.getCoordinate(), feature.getCoordinate() ) == 0
                        && Objects.equals( in.getLabel(), feature.getLabel() );
        boolean second = Objects.equals( in.getLocationId(), feature.getLocationId() )
                         && Objects.equals( in.getComid(), feature.getComid() )
                         && Objects.equals( in.getGageId(), feature.getGageId() );
        boolean third = Objects.equals( in.getHuc(), feature.getHuc() )
                        && Objects.equals( in.getName(), feature.getName() )
                        && Objects.equals( in.getRfc(), feature.getRfc() )
                        && Objects.equals( in.getWkt(), feature.getWkt() );
        return first && second && third;
    }

    @Override
    public int hashCode()
    {
        // Unwrap coordinates
        float[] coordinates = null;
        if ( Objects.nonNull( feature.getCoordinate() ) )
        {
            coordinates = new float[] { feature.getCoordinate().getLatitude(), feature.getCoordinate().getLongitude(),
                                        feature.getCoordinate().getRange() };
        }
        return Objects.hash( feature.getAlias(),
                             feature.getComid(),
                             Arrays.hashCode( coordinates ),
                             feature.getGageId(),
                             feature.getHuc(),
                             feature.getLabel(),
                             feature.getLocationId(),
                             feature.getName(),
                             feature.getRfc(),
                             feature.getWkt() );
    }

    @Override
    public int compareTo( FeaturePlus o )
    {
        // Compare the aliases
        Objects.requireNonNull( o, "Specify a non-null feature for comparison." );
        Feature input = o.getFeature();
        if ( !input.getAlias().containsAll( feature.getAlias() ) )
        {
            return -1;
        }
        else if ( !feature.getAlias().containsAll( input.getAlias() ) )
        {
            return 1;
        }
        // Compare the comid
        int returnMe = Objects.compare( input.getComid(), feature.getComid(), Comparator.naturalOrder() );
        if ( returnMe != 0 )
        {
            return returnMe;
        }
        // Compare coordinates
        returnMe = compareCoordinates( input.getCoordinate(), feature.getCoordinate() );
        if ( returnMe != 0 )
        {
            return returnMe;
        }
        // Compare gageId
        returnMe = Objects.compare( input.getGageId(), feature.getGageId(), Comparator.naturalOrder() );
        if ( returnMe != 0 )
        {
            return returnMe;
        }
        // Compare HUC
        returnMe = Objects.compare( input.getHuc(), feature.getHuc(), Comparator.naturalOrder() );
        if ( returnMe != 0 )
        {
            return returnMe;
        }
        // Compare label
        returnMe = Objects.compare( input.getLabel(), feature.getLabel(), Comparator.naturalOrder() );
        if ( returnMe != 0 )
        {
            return returnMe;
        }
        // Compare locationId
        returnMe = Objects.compare( input.getLocationId(), feature.getLocationId(), Comparator.naturalOrder() );
        if ( returnMe != 0 )
        {
            return returnMe;
        }
        // Compare name
        returnMe = Objects.compare( input.getName(), feature.getName(), Comparator.naturalOrder() );
        if ( returnMe != 0 )
        {
            return returnMe;
        }
        // Compare RFC
        returnMe = Objects.compare( input.getRfc(), feature.getRfc(), Comparator.naturalOrder() );
        if ( returnMe != 0 )
        {
            return returnMe;
        }
        // Compare Wkt
        returnMe = Objects.compare( input.getWkt(), feature.getWkt(), Comparator.naturalOrder() );
        if ( returnMe != 0 )
        {
            return returnMe;
        }
        return 0;
    }

    /**
     * Hidden constructor.
     * 
     * @param feature the {@link Feature} to wrap
     * @throws NullPointerException of the input is null
     */

    private FeaturePlus( Feature feature )
    {
        Objects.requireNonNull( feature, "Specify a non-null feature to wrap." );
        this.feature = feature;
    }


    /**
     * Compares two {@link CoordinateSelection}. Returns a negative number, zero, or a positive integer if the left
     * entry is less than, equal to, or greater than the right entry, respectively.
     * 
     * @param left the left coordinate
     * @param right the right coordinate
     * @return a negative integer, zero, or a positive integer as this object is less than, equal to, or greater 
     *            than the specified object.
     */

    private int compareCoordinates( CoordinateSelection left, CoordinateSelection right )
    {
        // Compare the coordinates
        if ( Objects.nonNull( left ) && Objects.isNull( right ) )
        {
            return -1;
        }
        else if ( Objects.isNull( left ) && Objects.nonNull( right ) )
        {
            return 1;
        }
        else if ( Objects.nonNull( right ) )
        {
            int returnMe = Float.compare( left.getLatitude(), right.getLatitude() );
            if ( returnMe != 0 )
            {
                return returnMe;
            }
            returnMe = Float.compare( left.getLongitude(), right.getLongitude() );
            if ( returnMe != 0 )
            {
                return returnMe;
            }
            returnMe = Float.compare( left.getRange(), right.getRange() );
            if ( returnMe != 0 )
            {
                return returnMe;
            }
        }
        return 0;
    }

}
