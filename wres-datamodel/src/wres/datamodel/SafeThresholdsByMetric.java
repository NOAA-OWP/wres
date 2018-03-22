package wres.datamodel;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.ThresholdConstants.ThresholdType;

/**
 * Immutable implementation of {@link ThresholdsByMetric}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class SafeThresholdsByMetric implements ThresholdsByMetric
{

    /**
     * Null metric error string.
     */

    private static final String NULL_METRIC_ERROR = "Specify a non-null metric.";

    /**
     * Null threshold error string.
     */

    private static final String NULL_THRESHOLD_ERROR = "Specify a non-null threshold.";

    /**
     * Null threshold type error string.
     */

    private static final String NULL_THRESHOLD_TYPE_ERROR = "Specify a non-null threshold type.";

    /**
     * Thresholds by {@link ThresholdType#PROBABILITY}.
     */

    private Map<MetricConstants, Set<Threshold>> probabilities = new EnumMap<>( MetricConstants.class );

    /**
     * Thresholds by {@link ThresholdType#VALUE}.
     */

    private Map<MetricConstants, Set<Threshold>> values = new EnumMap<>( MetricConstants.class );

    /**
     * Thresholds by {@link ThresholdType#PROBABILITY_CLASSIFIER}.
     */

    private Map<MetricConstants, Set<Threshold>> probabilityClassifiers = new EnumMap<>( MetricConstants.class );

    /**
     * Thresholds by {@link ThresholdType#QUANTILE}.
     */

    private Map<MetricConstants, Set<Threshold>> quantiles = new EnumMap<>( MetricConstants.class );

    @Override
    public Map<MetricConstants, Set<Threshold>> getThresholds( ThresholdType type )
    {
        Objects.requireNonNull( type, NULL_THRESHOLD_TYPE_ERROR );

        Map<MetricConstants, Set<Threshold>> returnMe = new EnumMap<>( MetricConstants.class );

        if ( type == ThresholdType.PROBABILITY )
        {
            returnMe.putAll( this.getProbabilities() );
        }
        else if ( type == ThresholdType.VALUE )
        {
            returnMe.putAll( this.getValues() );
        }
        else if ( type == ThresholdType.PROBABILITY_CLASSIFIER )
        {
            returnMe.putAll( this.getProbabilityClassifiers() );
        }
        else
        {
            returnMe.putAll( this.getQuantiles() );
        }

        return Collections.unmodifiableMap( returnMe );
    }

    @Override
    public boolean hasType( ThresholdType type )
    {
        Objects.requireNonNull( type, NULL_THRESHOLD_TYPE_ERROR );

        return !this.getThresholds( type ).isEmpty();
    }

    @Override
    public Set<Threshold> union()
    {
        Set<Threshold> union = new HashSet<>();

        // Iterate the types
        for ( ThresholdType nextType : ThresholdType.values() )
        {
            this.getThresholds( nextType ).values().forEach( union::addAll );
        }

        return Collections.unmodifiableSet( union );
    }

    @Override
    public Set<Threshold> unionForThisMetric( MetricConstants metric )
    {
        return this.unionForThisMetricAndTheseTypes( metric, ThresholdType.values() );
    }

    @Override
    public Set<Threshold> unionForTheseTypes( ThresholdType... type )
    {
        if ( Objects.isNull( type ) || type.length == 0 )
        {
            return Collections.emptySet();
        }

        Set<Threshold> union = new HashSet<>();

        // Iterate the types
        for ( ThresholdType nextType : ThresholdType.values() )
        {
            if ( Arrays.asList( type ).contains( nextType ) )
            {
                this.getThresholds( nextType ).values().forEach( union::addAll );
            }
        }

        return Collections.unmodifiableSet( union );
    }

    @Override
    public Set<Threshold> unionForThisMetricAndTheseTypes( MetricConstants metric, ThresholdType... type )
    {
        Objects.requireNonNull( metric, NULL_METRIC_ERROR );

        Set<Threshold> union = new HashSet<>();

        // Iterate the types if non-null
        if ( Objects.nonNull( type ) )
        {
            for ( ThresholdType nextType : type )
            {
                if ( this.getThresholds( nextType ).containsKey( metric ) )
                {
                    union.addAll( this.getThresholds( nextType ).get( metric ) );
                }
            }
        }

        return Collections.unmodifiableSet( union );
    }

    @Override
    public ThresholdsByMetric unionWithThisStore( ThresholdsByMetric thresholds )
    {
        Objects.requireNonNull( thresholds,
                                "Specify non-null input from which to form the union with these thresholds." );

        if ( thresholds == this )
        {
            return this;
        }

        SafeThresholdsByMetricBuilder builder = new SafeThresholdsByMetricBuilder();

        // Find the union for each type
        for ( ThresholdType nextType : ThresholdType.values() )
        {
            if ( this.hasType( nextType ) || thresholds.hasType( nextType ) )
            {
                Map<MetricConstants, Set<Threshold>> union = new EnumMap<>( MetricConstants.class );

                // Add the mutable sets for the existing container
                this.getThresholds( nextType ).forEach( ( key, value ) -> union.put( key, new HashSet<>( value ) ) );

                // Form union with input sets
                for ( Entry<MetricConstants, Set<Threshold>> next : thresholds.getThresholds( nextType ).entrySet() )
                {
                    if ( union.containsKey( next.getKey() ) )
                    {
                        union.get( next.getKey() ).addAll( next.getValue() );
                    }
                    else
                    {
                        union.put( next.getKey(), next.getValue() );
                    }
                }

                // Add to builder
                builder.addThresholds( union, nextType );
            }
        }

        return builder.build();
    }

    @Override
    public Set<OneOrTwoThresholds> unionOfOneOrTwoThresholds()
    {
        Set<OneOrTwoThresholds> returnMe = new HashSet<>();
        // Add all thresholds to the set
        this.getOneOrTwoThresholds().values().forEach( returnMe::addAll );

        return Collections.unmodifiableSet( returnMe );
    }

    @Override
    public Set<ThresholdType> getThresholdTypes()
    {
        Set<ThresholdType> types = new HashSet<>();

        for ( ThresholdType nextType : ThresholdType.values() )
        {
            if ( this.hasType( nextType ) )
            {
                types.add( nextType );
            }
        }

        return Collections.unmodifiableSet( types );
    }

    @Override
    public Set<ThresholdType> getThresholdTypesForThisMetric( MetricConstants metric )
    {
        Objects.requireNonNull( metric, NULL_METRIC_ERROR );

        Set<ThresholdType> types = new HashSet<>();

        // Iterate the types
        for ( ThresholdType nextType : ThresholdType.values() )
        {
            if ( this.getThresholds( nextType ).containsKey( metric ) )
            {
                types.add( nextType );
            }
        }

        return Collections.unmodifiableSet( types );
    }

    @Override
    public Set<MetricConstants> hasTheseMetricsForThisThreshold( Threshold threshold )
    {
        Objects.requireNonNull( threshold, NULL_THRESHOLD_ERROR );

        Set<MetricConstants> metrics = new HashSet<>();

        // Filter each set, collecting those metrics for which the input threshold is specified
        for ( ThresholdType nextType : ThresholdType.values() )
        {
            metrics.addAll( this.filterByThreshold( this.getThresholds( nextType ), threshold ) );
        }

        return Collections.unmodifiableSet( metrics );
    }

    @Override
    public Set<MetricConstants> doesNotHaveTheseMetricsForThisThreshold( Threshold threshold )
    {
        Objects.requireNonNull( threshold, NULL_THRESHOLD_ERROR );

        Set<MetricConstants> union = new HashSet<>( this.getMetrics() );

        // Remove all metrics that do have the threshold
        union.removeAll( this.hasTheseMetricsForThisThreshold( threshold ) );

        return Collections.unmodifiableSet( union );
    }

    @Override
    public Set<MetricConstants> hasThresholdsForTheseMetrics()
    {
        Set<MetricConstants> returnMe = new HashSet<>();

        for ( ThresholdType nextType : ThresholdType.values() )
        {
            returnMe.addAll( this.getThresholds( nextType ).keySet() );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    @Override
    public ThresholdsByMetric filterByType( ThresholdType... type )
    {
        if ( Objects.nonNull( type ) && Arrays.equals( type, ThresholdType.values() ) )
        {
            return this;
        }

        SafeThresholdsByMetricBuilder builder = new SafeThresholdsByMetricBuilder();

        // Add the stored types within the input array
        if ( Objects.nonNull( type ) )
        {
            for ( ThresholdType nextType : ThresholdType.values() )
            {
                // Filter by type
                if ( Arrays.asList( type ).contains( nextType ) )
                {
                    Map<MetricConstants, Set<Threshold>> thresholds = this.getThresholds( nextType );
                    builder.addThresholds( thresholds, nextType );
                }
            }
        }

        return builder.build();
    }

    @Override
    public ThresholdsByMetric filterByGroup( MetricInputGroup inGroup, MetricOutputGroup outGroup )
    {
        if ( Objects.isNull( inGroup ) && Objects.isNull( outGroup ) )
        {
            return this;
        }

        // Test the metric for membership of the groups
        Predicate<MetricConstants> test = metric -> {
            if ( Objects.nonNull( inGroup ) && Objects.nonNull( outGroup ) )
            {
                return metric.isInGroup( inGroup, outGroup );
            }
            else if ( Objects.nonNull( inGroup ) )
            {
                return metric.isInGroup( inGroup );
            }
            return metric.isInGroup( outGroup );
        };

        SafeThresholdsByMetricBuilder builder = new SafeThresholdsByMetricBuilder();

        // Add the filtered thresholds for each type
        for ( ThresholdType nextType : ThresholdType.values() )
        {
            this.addFilteredThresholdsByGroup( builder, nextType, this.getThresholds( nextType ), test );
        }

        return builder.build();
    }

    @Override
    public Map<MetricConstants, Set<OneOrTwoThresholds>> getOneOrTwoThresholds()
    {
        Map<MetricConstants, Set<OneOrTwoThresholds>> returnMe = new EnumMap<>( MetricConstants.class );

        // Find all stored metrics
        Set<MetricConstants> union = this.getMetrics();

        // Iterate the metrics
        for ( MetricConstants next : union )
        {
            //Non-classifiers
            Set<ThresholdType> types = new HashSet<>( this.getThresholdTypesForThisMetric( next ) );
            types.removeIf( type -> type == ThresholdType.PROBABILITY_CLASSIFIER );
            Set<Threshold> nonClassifiers =
                    this.unionForThisMetricAndTheseTypes( next, types.toArray( new ThresholdType[types.size()] ) );

            // Thresholds to add
            Set<OneOrTwoThresholds> oneOrTwo = new HashSet<>();

            // Dichotomous metrics with classifiers
            if ( next.isInGroup( MetricInputGroup.DICHOTOMOUS )
                 && this.hasType( ThresholdType.PROBABILITY_CLASSIFIER ) )
            {
                // Classifiers
                Set<Threshold> classifiers =
                        this.unionForThisMetricAndTheseTypes( next, ThresholdType.PROBABILITY_CLASSIFIER );

                for ( Threshold first : nonClassifiers )
                {
                    for ( Threshold second : classifiers )
                    {
                        oneOrTwo.add( OneOrTwoThresholds.of( first, second ) );
                    }
                }
            }
            // All other metrics
            else
            {
                for ( Threshold first : nonClassifiers )
                {
                    oneOrTwo.add( OneOrTwoThresholds.of( first ) );
                }
            }

            // Update container
            returnMe.put( next, Collections.unmodifiableSet( oneOrTwo ) );
        }

        return Collections.unmodifiableMap( returnMe );
    }

    /**
     * Builder.
     */

    static class SafeThresholdsByMetricBuilder implements ThresholdsByMetricBuilder
    {

        /**
         * Thresholds by {@link ThresholdType#PROBABILITY}.
         */

        private Map<MetricConstants, Set<Threshold>> probabilities = new EnumMap<>( MetricConstants.class );

        /**
         * Thresholds by {@link ThresholdType#VALUE}.
         */

        private Map<MetricConstants, Set<Threshold>> values = new EnumMap<>( MetricConstants.class );

        /**
         * Thresholds by {@link ThresholdType#PROBABILITY_CLASSIFIER}.
         */

        private Map<MetricConstants, Set<Threshold>> probabilityClassifiers = new EnumMap<>( MetricConstants.class );

        /**
         * Thresholds by {@link ThresholdType#QUANTILE}.
         */

        private Map<MetricConstants, Set<Threshold>> quantiles = new EnumMap<>( MetricConstants.class );

        @Override
        public ThresholdsByMetricBuilder addThresholds( Map<MetricConstants, Set<Threshold>> thresholds,
                                                        ThresholdType thresholdType )
        {
            Objects.requireNonNull( thresholds, "Cannot build a store of thresholds with null thresholds." );

            Objects.requireNonNull( thresholdType, "Cannot build a store of thresholds with null threshold type." );

            for ( Entry<MetricConstants, Set<Threshold>> nextEntry : thresholds.entrySet() )
            {

                Map<MetricConstants, Set<Threshold>> container = null;

                // Determine type of container
                if ( thresholdType == ThresholdType.PROBABILITY )
                {
                    container = this.probabilities;
                }
                else if ( thresholdType == ThresholdType.PROBABILITY_CLASSIFIER )
                {
                    container = this.probabilityClassifiers;
                }
                else if ( thresholdType == ThresholdType.QUANTILE )
                {
                    container = this.quantiles;
                }
                else if ( thresholdType == ThresholdType.VALUE )
                {
                    container = this.values;
                }
                else
                {
                    throw new IllegalArgumentException( "Unrecognized type of threshold '" + thresholdType + "'." );
                }

                // Append
                if ( container.containsKey( nextEntry.getKey() ) )
                {
                    container.get( nextEntry.getKey() ).addAll( nextEntry.getValue() );
                }
                // Add
                else
                {
                    container.put( nextEntry.getKey(), new HashSet<>( nextEntry.getValue() ) );
                }
            }

            return this;
        }

        @Override
        public ThresholdsByMetric build()
        {
            return new SafeThresholdsByMetric( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private SafeThresholdsByMetric( SafeThresholdsByMetricBuilder builder )
    {
        // Set immutable stores
        for ( Entry<MetricConstants, Set<Threshold>> next : builder.probabilities.entrySet() )
        {
            this.probabilities.put( next.getKey(), Collections.unmodifiableSet( new HashSet<>( next.getValue() ) ) );
        }

        for ( Entry<MetricConstants, Set<Threshold>> next : builder.values.entrySet() )
        {
            this.values.put( next.getKey(), Collections.unmodifiableSet( new HashSet<>( next.getValue() ) ) );
        }

        for ( Entry<MetricConstants, Set<Threshold>> next : builder.probabilityClassifiers.entrySet() )
        {
            this.probabilityClassifiers.put( next.getKey(),
                                             Collections.unmodifiableSet( new HashSet<>( next.getValue() ) ) );
        }

        for ( Entry<MetricConstants, Set<Threshold>> next : builder.quantiles.entrySet() )
        {
            this.quantiles.put( next.getKey(), Collections.unmodifiableSet( new HashSet<>( next.getValue() ) ) );
        }

        // Render the stores immutable       
        this.probabilities = Collections.unmodifiableMap( this.probabilities );
        this.probabilityClassifiers = Collections.unmodifiableMap( this.probabilityClassifiers );
        this.values = Collections.unmodifiableMap( this.values );
        this.quantiles = Collections.unmodifiableMap( this.quantiles );
    }

    /**
     * Return the probability thresholds.
     * 
     * @return the probability thresholds
     */

    private Map<MetricConstants, Set<Threshold>> getProbabilities()
    {
        return this.probabilities;
    }

    /**
     * Return the probability classifier thresholds.
     * 
     * @return the probability classifier thresholds
     */

    private Map<MetricConstants, Set<Threshold>> getProbabilityClassifiers()
    {
        return this.probabilityClassifiers;
    }

    /**
     * Return the value thresholds.
     * 
     * @return the value thresholds
     */

    private Map<MetricConstants, Set<Threshold>> getValues()
    {
        return this.values;
    }

    /**
     * Return the quantile thresholds.
     * 
     * @return the quantile thresholds
     */

    private Map<MetricConstants, Set<Threshold>> getQuantiles()
    {
        return this.quantiles;
    }

    /**
     * Returns a set of metrics for which the threshold exists in the specified container.
     * 
     * @param input the input container
     * @param threshold the threshold
     * @return the set of metrics
     * @throws NullPointerException if the input is null
     */

    private Set<MetricConstants> filterByThreshold( Map<MetricConstants, Set<Threshold>> input, Threshold threshold )
    {
        Objects.requireNonNull( input, "Specify non-null input" );

        return input.entrySet()
                    .stream()
                    .filter( entry -> entry.getValue().contains( threshold ) )
                    .map( Entry::getKey )
                    .collect( Collectors.toSet() );
    }


    /**
     * Mutates the builder, adding a new set of threshold results filtered for the input predicate.
     * 
     * @param builder the builder
     * @param type the type of threshold
     * @param thresholds the thresholds
     * @param test the predicate to indicate whether thresholds should be included
     */

    private void addFilteredThresholdsByGroup( SafeThresholdsByMetricBuilder builder,
                                               ThresholdType type,
                                               Map<MetricConstants, Set<Threshold>> thresholds,
                                               Predicate<MetricConstants> test )
    {

        Map<MetricConstants, Set<Threshold>> filtered = thresholds.entrySet()
                                                                  .stream()
                                                                  .filter( entry -> test.test( entry.getKey() ) )
                                                                  .collect( Collectors.toMap( Map.Entry::getKey,
                                                                                              Map.Entry::getValue ) );

        builder.addThresholds( filtered, type );
    }

    /**
     * Returns the full set of stored metrics.
     * 
     * @return the set of stored metrics
     */

    private Set<MetricConstants> getMetrics()
    {
        Set<MetricConstants> union = new HashSet<>();

        // Iterate the types
        for ( ThresholdType nextType : ThresholdType.values() )
        {
            union.addAll( this.getThresholds( nextType ).keySet() );
        }

        return Collections.unmodifiableSet( union );
    }

}
