package wres.datamodel.thresholds;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.Map.Entry;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.Immutable;
import wres.config.MetricConstants;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdGroup;

/**
 * A container of {@link ThresholdOuter} by {@link MetricConstants} that supports slicing and dicing by threshold and 
 * metric.
 * 
 * TODO: consider moving some of this behavior to the {@link ThresholdSlicer}.
 * 
 * @author James Brown
 */

@Immutable
public class ThresholdsByMetric
{

    private static final Logger LOGGER = LoggerFactory.getLogger( ThresholdsByMetric.class );

    /** Null metric error string. */
    private static final String NULL_METRIC_ERROR = "Specify a non-null metric.";

    /** Null threshold type error string. */
    private static final String NULL_THRESHOLD_TYPE_ERROR = "Specify a non-null threshold group type.";

    /** Thresholds by {@link ThresholdGroup#PROBABILITY}. */
    private final Map<MetricConstants, Set<ThresholdOuter>> probabilities;

    /** Thresholds by {@link ThresholdGroup#VALUE}. */
    private final Map<MetricConstants, Set<ThresholdOuter>> values;

    /** Thresholds by {@link ThresholdGroup#PROBABILITY_CLASSIFIER}. */
    private final Map<MetricConstants, Set<ThresholdOuter>> probabilityClassifiers;

    /** Thresholds by {@link ThresholdGroup#QUANTILE}. */
    private final Map<MetricConstants, Set<ThresholdOuter>> quantiles;

    /** Union of all metrics. */
    private final Set<MetricConstants> unionOfMetrics;

    /** Union of all thresholds. */
    private final Set<ThresholdOuter> unionOfThresholds;

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof ThresholdsByMetric ) )
        {
            return false;
        }

        if ( o == this )
        {
            return true;
        }

        ThresholdsByMetric in = (ThresholdsByMetric) o;

        return Objects.equals( this.probabilities, in.probabilities )
               && Objects.equals( this.probabilityClassifiers, in.probabilityClassifiers )
               && Objects.equals( this.values, in.values )
               && Objects.equals( this.quantiles, in.quantiles );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.probabilities, this.probabilityClassifiers, this.values, this.quantiles );
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
        return !this.union( metric, types )
                    .isEmpty();
    }

    /**
     * Returns the thresholds associated with a specified type. 
     * 
     * @param type the type of threshold
     * @return the thresholds for a specified type
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the type is unexpected
     */

    public Map<MetricConstants, Set<ThresholdOuter>> getThresholds( ThresholdGroup type )
    {
        Objects.requireNonNull( type, NULL_THRESHOLD_TYPE_ERROR );

        switch ( type )
        {
            case PROBABILITY:
                return this.getProbabilities();
            case VALUE:
                return this.getValues();
            case PROBABILITY_CLASSIFIER:
                return this.getProbabilityClassifiers();
            case QUANTILE:
                return this.getQuantiles();
            default:
                throw new IllegalArgumentException( "Unrecognized option '" + type + "'." );
        }
    }

    /**
     * Returns <code>true</code> if the store contains thresholds for the specified group, otherwise <code>false</code>.
     * 
     * @param group the type of threshold group
     * @return true if the store contains the type, otherwise false 
     * @throws NullPointerException if the input is null
     */

    public boolean hasGroup( ThresholdGroup group )
    {
        Objects.requireNonNull( group, NULL_THRESHOLD_TYPE_ERROR );

        return !this.getThresholds( group )
                    .isEmpty();
    }

    /**
     * <p>Returns the union of all thresholds in the store. Note that thresholds of type 
     * {@link ThresholdGroup#PROBABILITY} and {@link ThresholdGroup#PROBABILITY_CLASSIFIER} may contain exactly the same 
     * parameter values and hence may overlap. In general, this method should not be used for stores that contain 
     * both of these types.</p> 
     * 
     * <p>Also see {@link ThresholdSlicer#unionOfOneOrTwoThresholds(ThresholdsByMetric)}.</p> 
     * @return the union of all thresholds
     */

    public Set<ThresholdOuter> union()
    {
        return this.unionOfThresholds;
    }

    /**
     * Returns the union of all thresholds for a given metric and varargs of types. If no types are specified, returns 
     * the empty set.
     * 
     * @param metric the metric
     * @param group the threshold groups
     * @return the union of all thresholds for the specified metric and groups
     * @throws NullPointerException if the metric is null
     */

    public Set<ThresholdOuter> union( MetricConstants metric, ThresholdGroup... group )
    {
        Objects.requireNonNull( metric, NULL_METRIC_ERROR );

        Set<ThresholdOuter> union = new HashSet<>();

        // Iterate the types if non-null
        if ( Objects.nonNull( group ) )
        {
            for ( ThresholdGroup nextType : group )
            {
                Map<MetricConstants, Set<ThresholdOuter>> nextThresholds = this.getThresholds( nextType );
                if ( nextThresholds.containsKey( metric ) )
                {
                    union.addAll( nextThresholds.get( metric ) );
                }
            }
        }

        return Collections.unmodifiableSet( union );
    }

    /**
     * Returns the union of all thresholds for the given threshold types. If no types are specified, returns 
     * the empty set.
     * 
     * @param group the threshold groups
     * @return the union of all thresholds for the specified groups
     */

    public Set<ThresholdOuter> union( ThresholdGroup... group )
    {
        Set<ThresholdOuter> union = new HashSet<>();

        // Iterate the types if non-null
        if ( Objects.nonNull( group ) )
        {
            for ( ThresholdGroup nextType : group )
            {
                this.getThresholds( nextType )
                    .values()
                    .stream()
                    .flatMap( Set::stream )
                    .forEach( union::add );
            }
        }

        return Collections.unmodifiableSet( union );
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
            Map<MetricConstants, Set<ThresholdOuter>> nextThresholds = this.getThresholds( nextType );
            if ( nextThresholds.containsKey( metric ) )
            {
                types.add( nextType );
            }
        }

        return Collections.unmodifiableSet( types );
    }

    /**
     * Returns the set of metrics in the store.
     *
     * @return the stored metrics
     */

    public Set<MetricConstants> getMetrics()
    {
        return this.unionOfMetrics;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE ).append( "probabilityThresholds",
                                                                                     this.getProbabilities() )
                                                                            .append( "valueThresholds",
                                                                                     this.getValues() )
                                                                            .append( "quantileThresholds",
                                                                                     this.getQuantiles() )
                                                                            .append( "decisionThresholds",
                                                                                     this.getProbabilityClassifiers() )
                                                                            .toString();
    }

    /**
     * Builder.
     */

    public static class Builder
    {

        /**
         * Thresholds by {@link ThresholdGroup#PROBABILITY}.
         */

        private Map<MetricConstants, Set<ThresholdOuter>> probabilities = new EnumMap<>( MetricConstants.class );

        /**
         * Thresholds by {@link ThresholdGroup#VALUE}.
         */

        private Map<MetricConstants, Set<ThresholdOuter>> values = new EnumMap<>( MetricConstants.class );

        /**
         * Thresholds by {@link ThresholdGroup#PROBABILITY_CLASSIFIER}.
         */

        private Map<MetricConstants, Set<ThresholdOuter>> probabilityClassifiers =
                new EnumMap<>( MetricConstants.class );

        /**
         * Thresholds by {@link ThresholdGroup#QUANTILE}.
         */

        private Map<MetricConstants, Set<ThresholdOuter>> quantiles = new EnumMap<>( MetricConstants.class );

        /**
         * @return true if the builder contains no thresholds, otherwise false.
         */

        public boolean isEmpty()
        {
            boolean empty = this.values.values()
                                       .stream()
                                       .allMatch( Set::isEmpty );

            if ( empty )
            {
                empty = this.probabilities.values()
                                          .stream()
                                          .allMatch( Set::isEmpty );
            }

            if ( empty )
            {
                empty = this.probabilityClassifiers
                                                   .values()
                                                   .stream()
                                                   .allMatch( Set::isEmpty );
            }

            if ( empty )
            {
                empty = this.quantiles.values()
                                      .stream()
                                      .allMatch( Set::isEmpty );
            }

            return empty;
        }

        /**
         * Adds a map of thresholds. If any of the metrics do not support thresholds according to 
         * {@link MetricConstants#isAThresholdMetric()}, then the {@link ThresholdOuter#ALL_DATA} threshold is added for
         * that metric.
         * 
         * @param thresholds the thresholds
         * @param group the threshold group
         * @return the builder
         * @throws NullPointerException if any input is null
         */

        public Builder addThresholds( Map<MetricConstants, Set<ThresholdOuter>> thresholds,
                                      ThresholdGroup group )
        {
            Objects.requireNonNull( thresholds, "Cannot build a store of thresholds with null thresholds." );

            for ( Entry<MetricConstants, Set<ThresholdOuter>> nextEntry : thresholds.entrySet() )
            {
                this.addThresholds( group, nextEntry.getKey(), nextEntry.getValue() );
            }

            return this;
        }

        /**
         * Adds a threshold. If the metric does not support thresholds according to 
         * {@link MetricConstants#isAThresholdMetric()}, then the {@link ThresholdOuter#ALL_DATA} threshold is added 
         * instead.
         *
         * @param group the threshold group
         * @param metric the metric
         * @param threshold the threshold
         * @throws NullPointerException if any input is null
         * @return the builder
         */

        public Builder addThreshold( ThresholdGroup group,
                                     MetricConstants metric,
                                     ThresholdOuter threshold )
        {
            Objects.requireNonNull( metric, "Cannot build a store of thresholds with a null metric." );

            this.addThresholds( group, metric, Set.of( threshold ) );

            return this;
        }

        /**
         * Adds a map of thresholds. If any the metric does not support thresholds according to 
         * {@link MetricConstants#isAThresholdMetric()}, then the {@link ThresholdOuter#ALL_DATA} threshold is added 
         * instead.
         * 
         * @param group the threshold group
         * @param metric the metric
         * @param thresholds the thresholds
         * @return the builder
         * @throws NullPointerException if any input is null
         */

        public Builder addThresholds( ThresholdGroup group,
                                      MetricConstants metric,
                                      Set<ThresholdOuter> thresholds )
        {
            Objects.requireNonNull( thresholds, "Cannot build a store of thresholds with null thresholds." );

            Objects.requireNonNull( group, "Cannot build a store of thresholds with null threshold type." );

            Map<MetricConstants, Set<ThresholdOuter>> container;

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

            Set<ThresholdOuter> addMe = new HashSet<>();
            if ( metric.isAThresholdMetric() )
            {
                addMe.addAll( thresholds );
            }
            else
            {
                LOGGER.trace( "While building thresholds-by-metric, discovered metric {}, which does not support "
                              + "all threshold types. Adding the \"all data\" threshold for this metric.",
                              metric );

                addMe.add( ThresholdOuter.ALL_DATA );
            }

            // Append
            if ( container.containsKey( metric ) )
            {
                container.get( metric ).addAll( addMe );
            }
            // Add
            else
            {
                container.put( metric, addMe );
            }

            return this;
        }

        /**
         * Adds a {@link OneOrTwoThresholds to the builder}.
         * 
         * @param metric the metric
         * @param thresholds the thresholds to add
         * @return the builder
         * @throws NullPointerException if any input is null
         */

        public Builder addThreshold( MetricConstants metric,
                                     OneOrTwoThresholds thresholds )
        {
            Objects.requireNonNull( metric );
            Objects.requireNonNull( thresholds );

            if ( thresholds.hasTwo() )
            {
                this.addThreshold( ThresholdGroup.PROBABILITY_CLASSIFIER, metric, thresholds.second() );
            }

            if ( thresholds.first().isQuantile() )
            {
                this.addThreshold( ThresholdGroup.QUANTILE, metric, thresholds.first() );
            }
            else if ( thresholds.first().hasProbabilities() )
            {
                this.addThreshold( ThresholdGroup.PROBABILITY, metric, thresholds.first() );
            }
            else
            {
                this.addThreshold( ThresholdGroup.VALUE, metric, thresholds.first() );
            }

            return this;
        }

        /**
         * Adds a collection of thresholds to the builder.
         * @param thresholds the thresholds
         * @return the builder
         * @throws NullPointerException if the input is null
         */

        public Builder addThresholds( Map<MetricConstants, SortedSet<OneOrTwoThresholds>> thresholds )
        {
            Objects.requireNonNull( thresholds );

            for ( Map.Entry<MetricConstants, SortedSet<OneOrTwoThresholds>> next : thresholds.entrySet() )
            {
                MetricConstants nextMetric = next.getKey();
                SortedSet<OneOrTwoThresholds> nextThresholds = next.getValue();

                nextThresholds.stream()
                              .forEach( aThreshold -> this.addThreshold( nextMetric, aThreshold ) );
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
     * Return the probability thresholds.
     * 
     * @return the probability thresholds
     */

    private Map<MetricConstants, Set<ThresholdOuter>> getProbabilities()
    {
        return this.probabilities;
    }

    /**
     * Return the probability classifier thresholds.
     * 
     * @return the probability classifier thresholds
     */

    private Map<MetricConstants, Set<ThresholdOuter>> getProbabilityClassifiers()
    {
        return this.probabilityClassifiers;
    }

    /**
     * Return the value thresholds.
     * 
     * @return the value thresholds
     */

    private Map<MetricConstants, Set<ThresholdOuter>> getValues()
    {
        return this.values;
    }

    /**
     * Return the quantile thresholds.
     * 
     * @return the quantile thresholds
     */

    private Map<MetricConstants, Set<ThresholdOuter>> getQuantiles()
    {
        return this.quantiles;
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private ThresholdsByMetric( Builder builder )
    {
        // Set immutable stores
        Set<MetricConstants> metricsInner = new HashSet<>();
        Set<ThresholdOuter> thresholdsInner = new HashSet<>();
        Map<MetricConstants, Set<ThresholdOuter>> probabilitiesInner = new EnumMap<>( MetricConstants.class );
        Map<MetricConstants, Set<ThresholdOuter>> valuesInner = new EnumMap<>( MetricConstants.class );
        Map<MetricConstants, Set<ThresholdOuter>> probabilityClassifiersInner = new EnumMap<>( MetricConstants.class );
        Map<MetricConstants, Set<ThresholdOuter>> quantilesInner = new EnumMap<>( MetricConstants.class );

        for ( Entry<MetricConstants, Set<ThresholdOuter>> next : builder.probabilities.entrySet() )
        {
            MetricConstants nextMetric = next.getKey();
            Set<ThresholdOuter> nextThresholds = next.getValue();
            Set<ThresholdOuter> nextThresholdsCopy = Set.copyOf( nextThresholds );
            probabilitiesInner.put( nextMetric, nextThresholdsCopy );
            metricsInner.add( nextMetric );
            thresholdsInner.addAll( nextThresholds );
        }

        for ( Entry<MetricConstants, Set<ThresholdOuter>> next : builder.values.entrySet() )
        {
            MetricConstants nextMetric = next.getKey();
            Set<ThresholdOuter> nextThresholds = next.getValue();
            Set<ThresholdOuter> nextThresholdsCopy = Set.copyOf( nextThresholds );
            valuesInner.put( nextMetric, nextThresholdsCopy );
            metricsInner.add( nextMetric );
            thresholdsInner.addAll( nextThresholds );
        }

        for ( Entry<MetricConstants, Set<ThresholdOuter>> next : builder.probabilityClassifiers.entrySet() )
        {
            MetricConstants nextMetric = next.getKey();
            Set<ThresholdOuter> nextThresholds = next.getValue();
            Set<ThresholdOuter> nextThresholdsCopy = Set.copyOf( nextThresholds );
            probabilityClassifiersInner.put( nextMetric, nextThresholdsCopy );
            metricsInner.add( nextMetric );
            thresholdsInner.addAll( nextThresholds );
        }

        for ( Entry<MetricConstants, Set<ThresholdOuter>> next : builder.quantiles.entrySet() )
        {
            MetricConstants nextMetric = next.getKey();
            Set<ThresholdOuter> nextThresholds = next.getValue();
            Set<ThresholdOuter> nextThresholdsCopy = Set.copyOf( nextThresholds );
            quantilesInner.put( nextMetric, nextThresholdsCopy );
            metricsInner.add( nextMetric );
            thresholdsInner.addAll( nextThresholds );
        }

        // Render the stores immutable       
        this.probabilities = Collections.unmodifiableMap( probabilitiesInner );
        this.probabilityClassifiers = Collections.unmodifiableMap( probabilityClassifiersInner );
        this.values = Collections.unmodifiableMap( valuesInner );
        this.quantiles = Collections.unmodifiableMap( quantilesInner );
        this.unionOfMetrics = Collections.unmodifiableSet( metricsInner );
        this.unionOfThresholds = Collections.unmodifiableSet( thresholdsInner );
    }
}
