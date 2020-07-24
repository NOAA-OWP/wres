package wres.datamodel.statistics;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.ScoreStatistic.ScoreComponent;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * An abstract base class for an immutable score output.
 * 
 * @author james.brown@hydrosolved.com
 */

abstract class BasicScoreStatistic<S, T extends ScoreComponent<?>> implements ScoreStatistic<S, T>
{

    /**
     * Null output message.
     */

    private static final String NULL_OUTPUT_MESSAGE = "Specify a non-null statistic.";

    /**
     * Null metadata message.
     */

    private static final String NULL_METADATA_MESSAGE = "Specify non-null metadata for the statistic.";

    /**
     * The statistic.
     */

    private final S score;

    /**
     * A convenient mapping to internal types.
     */

    private final Map<MetricConstants, T> internal;

    /**
     * The metadata associated with the statistic.
     */

    private final SampleMetadata metadata;

    @Override
    public SampleMetadata getMetadata()
    {
        return this.metadata;
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( ! ( o instanceof BasicScoreStatistic ) )
        {
            return false;
        }

        BasicScoreStatistic<?, ?> v = (BasicScoreStatistic<?, ?>) o;
        boolean start = this.getMetadata().equals( v.getMetadata() );

        if ( !start )
        {
            return false;
        }

        return this.getData().equals( v.getData() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getData(), this.getMetadata() );
    }

    @Override
    public S getData()
    {
        return this.score;
    }

    @Override
    public Iterator<T> iterator()
    {
        return Collections.unmodifiableCollection( this.internal.values() ).iterator();
    }

    @Override
    public Set<MetricConstants> getComponents()
    {
        return Collections.unmodifiableSet( this.internal.keySet() );
    }

    @Override
    public boolean hasComponent( MetricConstants component )
    {
        return this.internal.containsKey( component );
    }

    @Override
    public T getComponent( MetricConstants component )
    {
        if ( !this.internal.containsKey( component ) )
        {
            throw new IllegalArgumentException( "The component " + component + " is not defined in this context." );
        }

        return this.internal.get( component );
    }

    /**
     * A wrapper for a {@link DoubleScoreStatisticComponent}.
     * 
     * @author james.brown@hydrosolved.com
     */

    abstract static class BasicScoreComponent<S> implements ScoreComponent<S>
    {

        /**
         * The component.
         */

        private final S component;

        /**
         * The metadata.
         */

        private final SampleMetadata metadata;
        
        /**
         * Mapper to a pretty string.
         */

        private final Function<S,String> mapper;

        /**
         * Hidden constructor.
         * @param component the score component
         * @param metadata the metadata
         * @param mapper a mapper to a pretty string
         * @throws NullPointerException if any input is null
         */

        BasicScoreComponent( S component,
                             SampleMetadata metadata,
                             Function<S,String> mapper )
        {
            Objects.requireNonNull( component );
            Objects.requireNonNull( metadata );
            Objects.requireNonNull( mapper );
            
            this.component = component;
            this.metadata = metadata;
            this.mapper = mapper;
        }

        @Override
        public S getData()
        {
            return this.component;
        }

        @Override
        public SampleMetadata getMetadata()
        {
            return this.metadata;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( o == this )
            {
                return true;
            }

            if ( o.getClass() != this.getClass() )
            {
                return false;
            }

            BasicScoreComponent<?> component = (BasicScoreComponent<?>) o;

            return Objects.equals( this.getMetricName(), component.getMetricName() )
                   && Objects.equals( this.getData(), component.getData() )
                   && Objects.equals( this.getMetadata(), component.getMetadata() );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( this.getMetricName(), this.getData(), this.getMetadata() );
        }

        @Override
        public String toString()
        {
            String pretty = this.mapper.apply( this.getData() );
            return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                                                                                .append( "name", this.getMetricName().name() )
                                                                                .append( "value", pretty )
                                                                                .build();
        }
    }

    /**
     * Returns the component mapping for building a friendly string representation of the score.
     * 
     * @return the component mapping
     */
    
    Map<MetricConstants, T> getInternalMapping()
    {
        return this.internal;
    }
    
    /**
     * Construct the output.
     * 
     * @param score the verification score
     * @param internal the internal mapping between score component names and component values
     * @param metadata the metadata
     */

    BasicScoreStatistic( S score, Map<MetricConstants, T> internal, SampleMetadata metadata )
    {
        Objects.requireNonNull( metadata, NULL_METADATA_MESSAGE );
        Objects.requireNonNull( score, NULL_OUTPUT_MESSAGE );

        this.score = score;
        this.metadata = metadata;
        this.internal = Collections.unmodifiableMap( internal );
    }

}
