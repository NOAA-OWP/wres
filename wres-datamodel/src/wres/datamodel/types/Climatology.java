package wres.datamodel.types;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.Immutable;

import wres.datamodel.MissingValues;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;

/**
 * Abstraction of a climatological dataset for one or more geographic features. The climatological data is sorted on 
 * construction in order of increasing size. Missing data is tolerated, including zero time-series events for one or
 * more features.
 *
 * @author James Brown
 */

@Immutable
public class Climatology
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( Climatology.class );

    /** The climatological data, mapped by feature. */
    private final Map<Feature, double[]> climateData;

    /** The measurement unit. */
    private final String measurementUnit;

    /**
     * Returns an instance.
     * @param timeSeries the time-series
     * @return an instance
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if there is no climatology for one or more features
     */

    public static Climatology of( List<TimeSeries<Double>> timeSeries )
    {
        return new Builder().addTimeSeries( timeSeries )
                            .build();
    }

    /**
     * Returns a cloned copy of the climatology for a named feature.
     *
     * @param feature the feature
     * @return a cloned copy of the climatology or null if the prescribed feature does not exist
     * @throws NullPointerException if the feature is null
     * @throws IllegalArgumentException if the feature does not exist in this context
     */

    public double[] get( Feature feature )
    {
        Objects.requireNonNull( feature );

        if ( this.hasNoClimatology( feature ) )
        {
            throw new IllegalArgumentException( "There is no climatology available for feature " + feature + "." );
        }

        return this.climateData.get( feature )
                               .clone();
    }

    /**
     * @return the measurement unit
     */
    public String getMeasurementUnit()
    {
        return this.measurementUnit;
    }

    /**
     * @return the features for which climatological data is available
     */

    public Set<Feature> getFeatures()
    {
        return Collections.unmodifiableSet( this.climateData.keySet() );
    }

    /**
     * @param feature the feature
     * @return whether a climatological dataset is missing for the specified feature
     */

    public boolean hasNoClimatology( Feature feature )
    {
        return !this.climateData.containsKey( feature );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( !( o instanceof Climatology in ) )
        {
            return false;
        }

        if ( o == this )
        {
            return true;
        }

        // Check the mapped items
        if ( in.climateData.size() != this.climateData.size() )
        {
            return false;
        }

        if ( !in.getMeasurementUnit()
                .equals( this.getMeasurementUnit() ) )
        {
            return false;
        }

        for ( Map.Entry<Feature, double[]> nextEntry : this.climateData.entrySet() )
        {
            if ( !Arrays.equals( nextEntry.getValue(), in.climateData.get( nextEntry.getKey() ) ) )
            {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int hash = 7;

        for ( Map.Entry<Feature, double[]> nextEntry : this.climateData.entrySet() )
        {
            hash = 31 * hash + Objects.hash( nextEntry.getKey() );
            hash = 31 * hash + Arrays.hashCode( nextEntry.getValue() );
        }

        hash = 31 * hash + this.getMeasurementUnit()
                               .hashCode();

        return hash;
    }

    @Override
    public String toString()
    {
        Map<Feature, String> stringified =
                this.climateData.entrySet()
                                .stream()
                                .map( next -> Map.entry( next.getKey(), Arrays.toString( next.getValue() ) ) )
                                .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );

        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE ).append( "byFeature", stringified )
                                                                            .toString();
    }

    /**
     * Builds a climatological dataset, merging with existing data as needed.
     */

    public static class Builder
    {
        /** The climatological data. */
        private final Map<Feature, double[]> climateData = new HashMap<>();

        /** The measurement unit. */
        private String measurementUnit;

        /**
         * Adds a list of time-series to the climatology.
         *
         * @param timeSeries the time-series
         * @return this builder
         */

        public Builder addTimeSeries( List<TimeSeries<Double>> timeSeries )
        {
            for ( TimeSeries<Double> nextSeries : timeSeries )
            {
                Feature nextFeature = nextSeries.getMetadata()
                                                .getFeature();

                double[] nextSeriesDoubles = nextSeries.getEvents()
                                                       .stream()
                                                       .mapToDouble( Event::getValue )
                                                       .toArray();

                if ( this.climateData.containsKey( nextFeature ) )
                {
                    this.mergeAdd( nextFeature, nextSeriesDoubles );
                }
                else
                {
                    // No need to clone the above array is created anew
                    this.climateData.put( nextFeature, nextSeriesDoubles );
                }

                this.measurementUnit = nextSeries.getMetadata()
                                                 .getUnit();
            }

            return this;
        }

        /**
         * Adds the climatology for a specific feature.
         * @param feature the feature
         * @param climatology the climatology
         * @param measurementUnit the measurement unit
         * @return the builder
         */

        public Builder addClimatology( Feature feature, double[] climatology, String measurementUnit )
        {
            if ( Objects.nonNull( feature ) && Objects.nonNull( climatology ) )
            {
                if ( this.climateData.containsKey( feature ) )
                {
                    this.mergeAdd( feature, climatology );
                }
                else
                {
                    // Clone
                    double[] cloned = climatology.clone();
                    this.climateData.put( feature, cloned );
                }

                this.measurementUnit = measurementUnit;
            }

            return this;
        }

        /**
         * Builds an instance
         *
         * @return a climatology
         */

        public Climatology build()
        {
            return new Climatology( this );
        }

        /**
         * Merges the input climatology with an existing climatology.
         * @param feature the feature
         * @param toMergeInto the data to merge
         */

        private void mergeAdd( Feature feature, double[] toMergeInto )
        {
            BiFunction<Feature, double[], double[]> merger = ( f, e ) -> {
                double[] start = this.climateData.get( feature );
                double[] merged = new double[start.length + toMergeInto.length];
                System.arraycopy( start, 0, merged, 0, start.length );
                System.arraycopy( toMergeInto, 0, merged, start.length, toMergeInto.length );

                return merged;
            };

            this.climateData.computeIfPresent( feature, merger );
        }
    }

    /**
     * Creates an instance.
     *
     * @param builder the builder
     * @throws NullPointerException if the input is null
     */

    private Climatology( Builder builder )
    {
        // No need to clone in this context because the builder clones
        Map<Feature, double[]> climatologyInner = new HashMap<>( builder.climateData );

        // Filter and sort
        for ( Map.Entry<Feature, double[]> nextEntry : climatologyInner.entrySet() )
        {
            Feature nextFeature = nextEntry.getKey();
            double[] nextDoubles = nextEntry.getValue();
            double[] filteredAndSorted = Arrays.stream( nextDoubles )
                                               .filter( MissingValues::isNotMissingValue )
                                               .sorted()
                                               .toArray();
            climatologyInner.replace( nextFeature, filteredAndSorted );
        }

        this.climateData = Collections.unmodifiableMap( climatologyInner );
        this.measurementUnit = builder.measurementUnit;

        // Validate
        this.validate( this.climateData, this.measurementUnit );

        LOGGER.debug( "Created a climatological data source for features {}.", this.climateData.keySet() );
    }

    /**
     * Validates the climatological input.
     *
     * @param climatology the climatology
     * @param measurementUnit the measurement unit
     * @throws IllegalArgumentException if the climatology is missing for any feature
     */

    private void validate( Map<Feature, double[]> climatology, String measurementUnit )
    {
        Set<Feature> invalid = climatology.entrySet()
                                          .stream()
                                          .filter( next -> next.getValue().length == 0
                                                           || Arrays.stream( next.getValue() )
                                                                    .noneMatch( Double::isFinite ) )
                                          .map( Map.Entry::getKey )
                                          .collect( Collectors.toSet() );

        if ( !invalid.isEmpty()
             && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "When attempting to create a climatological dataset for {} feature(s), discovered that "
                         + "time-series data was missing for {} of these feature(s), as follows: {}",
                         climatology.keySet()
                                    .size(),
                         invalid.size(),
                         invalid );
        }

        if ( Objects.isNull( measurementUnit ) )
        {
            throw new IllegalArgumentException( "Cannot build a climatology without a measurement unit. " );
        }
    }

}
