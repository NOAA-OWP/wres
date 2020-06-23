package wres.datamodel.thresholds;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.SampleDataGroup;
import wres.datamodel.MetricConstants.StatisticType;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdGroup;

/**
 * A container of {@link Threshold} by {@link MetricConstants}. Includes an optional builder.
 * 
 * @author james.brown@hydrosolved.com
 */

public class ThresholdsByMetric
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
     * Thresholds by {@link ThresholdGroup#PROBABILITY}.
     */

    private Map<MetricConstants, Set<Threshold>> probabilities = new EnumMap<>( MetricConstants.class );

    /**
     * Thresholds by {@link ThresholdGroup#VALUE}.
     */

    private Map<MetricConstants, Set<Threshold>> values = new EnumMap<>( MetricConstants.class );

    /**
     * Thresholds by {@link ThresholdGroup#PROBABILITY_CLASSIFIER}.
     */

    private Map<MetricConstants, Set<Threshold>> probabilityClassifiers = new EnumMap<>( MetricConstants.class );

    /**
     * Thresholds by {@link ThresholdGroup#QUANTILE}.
     */

    private Map<MetricConstants, Set<Threshold>> quantiles = new EnumMap<>( MetricConstants.class );

    /**
     * <p>Returns a filtered view that contains the union of thresholds associated with metrics that belong to the 
     * specified group.</p> 
     * 
     * <p>See {@link #filterByGroup(MetricConstants.SampleDataGroup, MetricConstants.StatisticType)} also.</p> 
     * 
     * @param inGroup the input group
     * @return a filtered view by group
     * @throws NullPointerException if the input is null
     */

    public ThresholdsByMetric filterByGroup( SampleDataGroup inGroup )
    {
        Objects.requireNonNull( inGroup, "Specify a non-null input group on which to filter." );

        return this.filterByGroup( inGroup, null );
    }

    /**
     * <p>Returns a filtered view that contains the union of thresholds associated with metrics that belong to the 
     * specified group.</p>
     * 
     * <p>See {@link #filterByGroup(MetricConstants.SampleDataGroup, MetricConstants.StatisticType)} also.</p>
     * 
     * @param outGroup the optional output group
     * @return a filtered view by group
     * @throws NullPointerException if the input is null
     */

    public ThresholdsByMetric filterByGroup( StatisticType outGroup )
    {
        Objects.requireNonNull( outGroup, "Specify a non-null output group on which to filter." );

        return this.filterByGroup( null, outGroup );
    }

    /**
     * Returns <code>true</code> if the store contains thresholds for the input metric with one or more of
     * the specified types, otherwise <code>false</code>. If no types are specified, returns false.
     * 
     * @param metric the metric
     * @param types the threshold types
     * @return true if the store contains thresholds for the specified metric and one or more of the types
     * @throws NullPointerException if the metric is null
     */

    public boolean hasThresholdsForThisMetricAndTheseTypes( MetricConstants metric, ThresholdGroup... types )
    {
        if ( Objects.isNull( types ) || types.length == 0 )
        {
            return false;
        }
        return !this.unionForThisMetricAndTheseTypes( metric, types ).isEmpty();
    }

    /**
     * Returns the thresholds associated with a specified type. 
     * 
     * @param type the type of threshold
     * @return the thresholds for a specified type
     * @throws NullPointerException if the input is null 
     */

    public Map<MetricConstants, Set<Threshold>> getThresholds( ThresholdGroup type )
    {
        Objects.requireNonNull( type, NULL_THRESHOLD_TYPE_ERROR );

        Map<MetricConstants, Set<Threshold>> returnMe = new EnumMap<>( MetricConstants.class );

        if ( type == ThresholdGroup.PROBABILITY )
        {
            returnMe.putAll( this.getProbabilities() );
        }
        else if ( type == ThresholdGroup.VALUE )
        {
            returnMe.putAll( this.getValues() );
        }
        else if ( type == ThresholdGroup.PROBABILITY_CLASSIFIER )
        {
            returnMe.putAll( this.getProbabilityClassifiers() );
        }
        else
        {
            returnMe.putAll( this.getQuantiles() );
        }

        return Collections.unmodifiableMap( returnMe );
    }

    /**
     * <p>Returns the composed thresholds associated with each metric in the container. A composed threshold is a 
     * {@link OneOrTwoThresholds} that contains two thresholds if the metric consumes 
     * {@link SampleDataGroup#DICHOTOMOUS} and has {@link ThresholdGroup#PROBABILITY_CLASSIFIER},
     * otherwise one threshold. The thresholds are stored in natural order.
     * 
     * <p>Also see: {@link #unionOfOneOrTwoThresholds()}.</p>
     * 
     * @return the composed thresholds
     */

    public Map<MetricConstants, SortedSet<OneOrTwoThresholds>> getOneOrTwoThresholds()
    {
        Map<MetricConstants, SortedSet<OneOrTwoThresholds>> returnMe = new EnumMap<>( MetricConstants.class );

        // Find all stored metrics
        Set<MetricConstants> union = this.getMetrics();

        // Iterate the metrics
        for ( MetricConstants next : union )
        {
            //Non-classifiers
            Set<ThresholdGroup> types = new HashSet<>( this.getThresholdTypesForThisMetric( next ) );
            types.removeIf( type -> type == ThresholdGroup.PROBABILITY_CLASSIFIER );
            Set<Threshold> nonClassifiers =
                    this.unionForThisMetricAndTheseTypes( next, types.toArray( new ThresholdGroup[types.size()] ) );

            // Thresholds to add
            SortedSet<OneOrTwoThresholds> oneOrTwo = new TreeSet<>();

            // Dichotomous metrics with classifiers
            if ( next.isInGroup( SampleDataGroup.DICHOTOMOUS )
                 && this.hasType( ThresholdGroup.PROBABILITY_CLASSIFIER ) )
            {
                // Classifiers
                Set<Threshold> classifiers =
                        this.unionForThisMetricAndTheseTypes( next, ThresholdGroup.PROBABILITY_CLASSIFIER );

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
            returnMe.put( next, Collections.unmodifiableSortedSet( oneOrTwo ) );
        }

        return Collections.unmodifiableMap( returnMe );
    }

    /**
     * Returns <code>true</code> if the store contains thresholds for the specified group, otherwise <code>false</code>.
     * 
     * @param type the type of threshold group
     * @return true if the store contains the type, otherwise false 
     * @throws NullPointerException if the input is null
     */

    public boolean hasType( ThresholdGroup type )
    {
        Objects.requireNonNull( type, NULL_THRESHOLD_TYPE_ERROR );

        return !this.getThresholds( type ).isEmpty();
    }

    /**
     * <p>Returns the union of all thresholds in the store. Note that thresholds of type 
     * {@link ThresholdGroup#PROBABILITY} and {@link ThresholdGroup#PROBABILITY_CLASSIFIER} may contain exactly the same 
     * parameter values and hence may overlap. In general, this method should not be used for stores that contain 
     * both of these types.</p> 
     * 
     * <p>Also see {@link #unionOfOneOrTwoThresholds()}.</p> 
     * 
     * @return the union of all thresholds
     */

    public Set<Threshold> union()
    {
        Set<Threshold> union = new HashSet<>();

        // Iterate the types
        for ( ThresholdGroup nextType : ThresholdGroup.values() )
        {
            this.getThresholds( nextType ).values().forEach( union::addAll );
        }

        return Collections.unmodifiableSet( union );
    }

    /**
     * Returns the union of all thresholds returned by {@link #getOneOrTwoThresholds()}.
     * 
     * @return the union of all composed thresholds
     */

    public Set<OneOrTwoThresholds> unionOfOneOrTwoThresholds()
    {
        Set<OneOrTwoThresholds> returnMe = new HashSet<>();
        // Add all thresholds to the set
        this.getOneOrTwoThresholds().values().forEach( returnMe::addAll );

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Returns the union of all thresholds for a given metric.
     * 
     * @param metric the metric
     * @return the union of all thresholds for the specified metric
     * @throws NullPointerException if the input is null
     */

    public Set<Threshold> unionForThisMetric( MetricConstants metric )
    {
        return this.unionForThisMetricAndTheseTypes( metric, ThresholdGroup.values() );
    }

    /**
     * Returns the union of all thresholds for one or more types. If no types are specified, returns the empty set.
     * 
     * @param type the type of threshold
     * @return the thresholds for a specified type
     */

    public Set<Threshold> unionForTheseTypes( ThresholdGroup... type )
    {
        if ( Objects.isNull( type ) || type.length == 0 )
        {
            return Collections.emptySet();
        }

        Set<Threshold> union = new HashSet<>();

        // Iterate the types
        for ( ThresholdGroup nextType : ThresholdGroup.values() )
        {
            if ( Arrays.asList( type ).contains( nextType ) )
            {
                this.getThresholds( nextType ).values().forEach( union::addAll );
            }
        }

        return Collections.unmodifiableSet( union );
    }

    /**
     * Returns the union of all thresholds for a given metric and varargs of types. If no types are specified, returns 
     * the empty set.
     * 
     * @param metric the metric
     * @param type the threshold types
     * @return the union of all thresholds for the specified metric and types
     * @throws NullPointerException if the metric is null
     */

    public Set<Threshold> unionForThisMetricAndTheseTypes( MetricConstants metric, ThresholdGroup... type )
    {
        Objects.requireNonNull( metric, NULL_METRIC_ERROR );

        Set<Threshold> union = new HashSet<>();

        // Iterate the types if non-null
        if ( Objects.nonNull( type ) )
        {
            for ( ThresholdGroup nextType : type )
            {
                if ( this.getThresholds( nextType ).containsKey( metric ) )
                {
                    union.addAll( this.getThresholds( nextType ).get( metric ) );
                }
            }
        }

        return Collections.unmodifiableSet( union );
    }

    /**
     * Combines the input with the contents of the current container, return a new container that reflects the union
     * of the two.
     * 
     * @param thresholds the thresholds
     * @return the union of the input and the current thresholds
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the input store has thresholds for the same metrics 
     *            but with different application types
     */

    public ThresholdsByMetric unionWithThisStore( ThresholdsByMetric thresholds )
    {
        Objects.requireNonNull( thresholds,
                                "Specify non-null input from which to form the union with these thresholds." );

        if ( thresholds == this )
        {
            return this;
        }

        ThresholdsByMetricBuilder builder = new ThresholdsByMetricBuilder();

        // Find the union for each type
        for ( ThresholdGroup nextType : ThresholdGroup.values() )
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

    /**
     * Returns the set of {@link ThresholdGroup} in the store.
     * 
     * @return the threshold types stored
     */

    public Set<ThresholdGroup> getThresholdTypes()
    {
        Set<ThresholdGroup> types = new HashSet<>();

        for ( ThresholdGroup nextType : ThresholdGroup.values() )
        {
            if ( this.hasType( nextType ) )
            {
                types.add( nextType );
            }
        }

        return Collections.unmodifiableSet( types );
    }

    /**
     * Returns the type of thresholds associated with a given metric.
     * 
     * @param metric the metric 
     * @return the type of thresholds 
     * @throws NullPointerException if the input is null
     */

    public Set<ThresholdGroup> getThresholdTypesForThisMetric( MetricConstants metric )
    {
        Objects.requireNonNull( metric, NULL_METRIC_ERROR );

        Set<ThresholdGroup> types = new HashSet<>();

        // Iterate the types
        for ( ThresholdGroup nextType : ThresholdGroup.values() )
        {
            if ( this.getThresholds( nextType ).containsKey( metric ) )
            {
                types.add( nextType );
            }
        }

        return Collections.unmodifiableSet( types );
    }

    /**
     * Returns the metrics in the store for which the input threshold is defined.
     * 
     * @param threshold the threshold
     * @return the set of metrics for which the input threshold is defined
     * @throws NullPointerException if the input is null
     */

    public Set<MetricConstants> hasTheseMetricsForThisThreshold( Threshold threshold )
    {
        Objects.requireNonNull( threshold, NULL_THRESHOLD_ERROR );

        Set<MetricConstants> metrics = new HashSet<>();

        // Filter each set, collecting those metrics for which the input threshold is specified
        for ( ThresholdGroup nextType : ThresholdGroup.values() )
        {
            metrics.addAll( this.filterByThreshold( this.getThresholds( nextType ), threshold ) );
        }

        return Collections.unmodifiableSet( metrics );
    }

    /**
     * Returns the metrics in the store for which the input threshold is not defined.
     * 
     * @param threshold the threshold
     * @return the set of metrics for which the input threshold is not defined
     * @throws NullPointerException if the input is null
     */

    public Set<MetricConstants> doesNotHaveTheseMetricsForThisThreshold( Threshold threshold )
    {
        Objects.requireNonNull( threshold, NULL_THRESHOLD_ERROR );

        Set<MetricConstants> union = new HashSet<>( this.getMetrics() );

        // Remove all metrics that do have the threshold
        union.removeAll( this.hasTheseMetricsForThisThreshold( threshold ) );

        return Collections.unmodifiableSet( union );
    }

    /**
     * Returns the set of metrics in the store.
     *
     * @return the stored metrics
     */

    public Set<MetricConstants> hasThresholdsForTheseMetrics()
    {
        Set<MetricConstants> returnMe = new HashSet<>();

        for ( ThresholdGroup nextType : ThresholdGroup.values() )
        {
            returnMe.addAll( this.getThresholds( nextType ).keySet() );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Returns a filtered view that contains the union of thresholds for the given input types. If no types are 
     * defined, returns the empty set. If all types are defined, returns this container.
     * 
     * @param type the types
     * @return a filtered view by type
     */

    public ThresholdsByMetric filterByType( ThresholdGroup... type )
    {
        if ( Objects.nonNull( type ) && Arrays.equals( type, ThresholdGroup.values() ) )
        {
            return this;
        }

        ThresholdsByMetricBuilder builder = new ThresholdsByMetricBuilder();

        // Add the stored types within the input array
        if ( Objects.nonNull( type ) )
        {
            for ( ThresholdGroup nextType : ThresholdGroup.values() )
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

    /**
     * Returns a filtered view that contains the union of thresholds associated with metrics that belong to the 
     * specified groups. If both inputs are null, returns the current container.
     * 
     * @param inGroup the optional input group
     * @param outGroup the optional output group
     * @return a filtered view by group
     */

    public ThresholdsByMetric filterByGroup( SampleDataGroup inGroup, StatisticType outGroup )
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

        ThresholdsByMetricBuilder builder = new ThresholdsByMetricBuilder();

        // Add the filtered thresholds for each type
        for ( ThresholdGroup nextType : ThresholdGroup.values() )
        {
            this.addFilteredThresholdsByGroup( builder, nextType, this.getThresholds( nextType ), test );
        }

        return builder.build();
    }
    
    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner( System.lineSeparator() );
        
        joiner.add( "ThresholdsByMetric@" + this.hashCode() );
        joiner.add( "    Event probability thresholds: " + this.getProbabilities() );
        joiner.add( "    Event value thresholds: " + this.getValues() );
        joiner.add( "    Event quantile thresholds: " + this.getQuantiles() );
        joiner.add( "    Decision thresholds: " + this.getProbabilityClassifiers() );
        
        return joiner.toString(); 
    }
    
    /**
     * Builder.
     */

    public static class ThresholdsByMetricBuilder
    {

        /**
         * Thresholds by {@link ThresholdGroup#PROBABILITY}.
         */

        private Map<MetricConstants, Set<Threshold>> probabilities = new EnumMap<>( MetricConstants.class );

        /**
         * Thresholds by {@link ThresholdGroup#VALUE}.
         */

        private Map<MetricConstants, Set<Threshold>> values = new EnumMap<>( MetricConstants.class );

        /**
         * Thresholds by {@link ThresholdGroup#PROBABILITY_CLASSIFIER}.
         */

        private Map<MetricConstants, Set<Threshold>> probabilityClassifiers = new EnumMap<>( MetricConstants.class );

        /**
         * Thresholds by {@link ThresholdGroup#QUANTILE}.
         */

        private Map<MetricConstants, Set<Threshold>> quantiles = new EnumMap<>( MetricConstants.class );

        public boolean isEmpty() {
            boolean empty = this.values.values().stream().allMatch(Set::isEmpty);

            if (empty) {
                empty = this.probabilities.values().stream().allMatch(Set::isEmpty);
            }

            if (empty) {
                empty = this.probabilityClassifiers.values().stream().allMatch(Set::isEmpty);
            }

            if (empty) {
                empty = this.quantiles.values().stream().allMatch(Set::isEmpty);
            }

            return empty;
        }

        /**
         * Adds a map of thresholds.
         * 
         * @param thresholds the thresholds
         * @param thresholdType the threshold type
         * @return the builder
         * @throws NullPointerException if any input is null
         */

        public ThresholdsByMetricBuilder addThresholds( Map<MetricConstants, Set<Threshold>> thresholds,
                                                        ThresholdGroup thresholdType )
        {
            Objects.requireNonNull( thresholds, "Cannot build a store of thresholds with null thresholds." );

            Objects.requireNonNull( thresholdType, "Cannot build a store of thresholds with null threshold type." );

            for ( Entry<MetricConstants, Set<Threshold>> nextEntry : thresholds.entrySet() )
            {

                Map<MetricConstants, Set<Threshold>> container = null;

                // Determine type of container
                if ( thresholdType == ThresholdGroup.PROBABILITY )
                {
                    container = this.probabilities;
                }
                else if ( thresholdType == ThresholdGroup.PROBABILITY_CLASSIFIER )
                {
                    container = this.probabilityClassifiers;
                }
                else if ( thresholdType == ThresholdGroup.QUANTILE )
                {
                    container = this.quantiles;
                }
                else if ( thresholdType == ThresholdGroup.VALUE )
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

        /**
         * Adds a map of thresholds.
         *
         * @throws NullPointerException if any input is null
         */

        public ThresholdsByMetricBuilder addThreshold(ThresholdGroup group, MetricConstants metric, Threshold threshold)
        {
            Objects.requireNonNull( threshold, "Cannot build a store of thresholds with null thresholds." );

            Objects.requireNonNull( group, "Cannot build a store of thresholds with null threshold type." );

            Map<MetricConstants, Set<Threshold>> container;

            // Determine type of container
            if ( group == ThresholdGroup.PROBABILITY )
            {
                container = this.probabilities;
            }
            else if ( group == ThresholdGroup.PROBABILITY_CLASSIFIER )
            {
                container = this.probabilityClassifiers;
            }
            else if ( group == ThresholdGroup.QUANTILE )
            {
                container = this.quantiles;
            }
            else if ( group == ThresholdGroup.VALUE )
            {
                container = this.values;
            }
            else
            {
                throw new IllegalArgumentException( "Unrecognized type of threshold '" + group + "'." );
            }

            // Append
            if ( container.containsKey( metric ) )
            {
                container.get( metric ).add( threshold );
            }
            // Add
            else
            {
                container.put( metric, new HashSet<>(){{add(threshold);}});
            }

            return this;
        }

        /**
         * Adds a map of thresholds.
         *
         * @throws NullPointerException if any input is null
         */

        public ThresholdsByMetricBuilder addThresholds(ThresholdGroup group, MetricConstants metric, Set<Threshold> thresholds)
        {
            Objects.requireNonNull( thresholds, "Cannot build a store of thresholds with null thresholds." );

            Objects.requireNonNull( group, "Cannot build a store of thresholds with null threshold type." );

            Map<MetricConstants, Set<Threshold>> container;

            // Determine type of container
            if ( group == ThresholdGroup.PROBABILITY )
            {
                container = this.probabilities;
            }
            else if ( group == ThresholdGroup.PROBABILITY_CLASSIFIER )
            {
                container = this.probabilityClassifiers;
            }
            else if ( group == ThresholdGroup.QUANTILE )
            {
                container = this.quantiles;
            }
            else if ( group == ThresholdGroup.VALUE )
            {
                container = this.values;
            }
            else
            {
                throw new IllegalArgumentException( "Unrecognized type of threshold '" + group + "'." );
            }

            // Append
            if ( container.containsKey( metric ) )
            {
                container.get( metric ).addAll(thresholds);
            }
            // Add
            else
            {
                container.put( metric, new HashSet<>(){{addAll(thresholds);}});
            }

            return this;
        }

        /**
         * Builds a {@link ThresholdsByMetric}.
         * 
         * @return the container
         */

        public ThresholdsByMetric build()
        {
            return new ThresholdsByMetric( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private ThresholdsByMetric( ThresholdsByMetricBuilder builder )
    {
        // Set immutable stores
        for ( Entry<MetricConstants, Set<Threshold>> next : builder.probabilities.entrySet() )
        {
            this.probabilities.put( next.getKey(), Set.copyOf(next.getValue()));
        }

        for ( Entry<MetricConstants, Set<Threshold>> next : builder.values.entrySet() )
        {
            this.values.put( next.getKey(), Set.copyOf(next.getValue()));
        }

        for ( Entry<MetricConstants, Set<Threshold>> next : builder.probabilityClassifiers.entrySet() )
        {
            this.probabilityClassifiers.put( next.getKey(), Set.copyOf(next.getValue()));
        }

        for ( Entry<MetricConstants, Set<Threshold>> next : builder.quantiles.entrySet() )
        {
            this.quantiles.put( next.getKey(), Set.copyOf(next.getValue()));
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

    private void addFilteredThresholdsByGroup( ThresholdsByMetricBuilder builder,
                                               ThresholdGroup type,
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
        for ( ThresholdGroup nextType : ThresholdGroup.values() )
        {
            union.addAll( this.getThresholds( nextType ).keySet() );
        }

        return Collections.unmodifiableSet( union );
    }


}
