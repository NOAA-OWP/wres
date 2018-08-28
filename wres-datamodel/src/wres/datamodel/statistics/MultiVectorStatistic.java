package wres.datamodel.statistics;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metadata.StatisticMetadata;

/**
 * One or more vectors that are explicitly mapped to elements in {@link MetricDimension}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class MultiVectorStatistic implements Statistic<Map<MetricDimension, VectorOfDoubles>>
{
    /**
     * The statistic.
     */

    private final EnumMap<MetricDimension, VectorOfDoubles> statistic;

    /**
     * The metadata associated with the statistic.
     */

    private final StatisticMetadata meta;

    /**
     * Construct the statistic.
     * 
     * @param statistic the verification statistic
     * @param meta the metadata
     * @throws StatisticException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static MultiVectorStatistic of( final Map<MetricDimension, VectorOfDoubles> statistic,
                                           final StatisticMetadata meta )
    {
        return new MultiVectorStatistic( statistic, meta );
    }


    /**
     * Return a {@link MultiVectorStatistic}.
     * 
     * @param statistic the statistic data
     * @param meta the metadata
     * @return a {@link MultiVectorStatistic}
     * @throws NullPointerException if the statistic is null
     */

    public static MultiVectorStatistic ofMultiVectorOutput( Map<MetricDimension, double[]> statistic,
                                                            StatisticMetadata meta )
    {
        Objects.requireNonNull( statistic, "Specify a non-null map of inputs." );

        EnumMap<MetricDimension, VectorOfDoubles> map = new EnumMap<>( MetricDimension.class );
        statistic.forEach( ( key, value ) -> map.put( key, VectorOfDoubles.of( value ) ) );

        return of( map, meta );
    }

    /**
     * Returns a prescribed vector from the map or null if no mapping exists.
     * 
     * @param identifier the identifier
     * @return a vector or null
     */

    public VectorOfDoubles get( MetricDimension identifier )
    {
        return statistic.get( identifier );
    }

    /**
     * Returns true if the store contains a mapping for the prescribed identifier, false otherwise.
     * 
     * @param identifier the identifier
     * @return true if the mapping exists, false otherwise
     */

    public boolean containsKey( MetricDimension identifier )
    {
        return statistic.containsKey( identifier );
    }

    @Override
    public StatisticMetadata getMetadata()
    {
        return meta;
    }

    @Override
    public Map<MetricDimension, VectorOfDoubles> getData()
    {
        return new EnumMap<>( statistic );
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( ! ( o instanceof MultiVectorStatistic ) )
        {
            return false;
        }
        final MultiVectorStatistic v = (MultiVectorStatistic) o;
        return meta.equals( v.getMetadata() ) && statistic.equals( v.statistic );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( meta, statistic );
    }

    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner( System.lineSeparator() );
        statistic.forEach( ( key, value ) -> joiner.add( key + ": " + value ) );
        return joiner.toString();
    }

    /**
     * Construct the statistic.
     * 
     * @param statistic the verification statistic
     * @param meta the metadata
     * @throws StatisticException if any of the inputs are invalid
     */

    private MultiVectorStatistic( final Map<MetricDimension, VectorOfDoubles> statistic, final StatisticMetadata meta )
    {
        if ( Objects.isNull( statistic ) )
        {
            throw new StatisticException( "Specify a non-null output." );
        }
        if ( Objects.isNull( meta ) )
        {
            throw new StatisticException( "Specify non-null metadata." );
        }
        if ( statistic.isEmpty() )
        {
            throw new StatisticException( "Specify one or more outputs to store." );
        }
        this.statistic = new EnumMap<>( MetricDimension.class );
        statistic.forEach( ( key, value ) -> this.statistic.put( key, value ) );
        this.meta = meta;
    }

}
