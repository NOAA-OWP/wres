package wres.datamodel;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.ThresholdConstants.ThresholdType;

/**
 * A container of {@link Threshold} by {@link MetricConstants}. Includes an optional builder.
 * 
 * @author james.brown@hydrosolved.com
 */

public interface ThresholdsByMetric
{

    /**
     * <p>Returns a filtered view that contains the union of thresholds associated with metrics that belong to the 
     * specified group.</p> 
     * 
     * <p>See {@link #filterByGroup(MetricConstants.MetricInputGroup, MetricConstants.MetricOutputGroup)} also.</p> 
     * 
     * @param inGroup the input group
     * @return a filtered view by group
     * @throws NullPointerException if the input is null
     */

    default ThresholdsByMetric filterByGroup( MetricInputGroup inGroup )
    {
        Objects.requireNonNull( inGroup, "Specify a non-null input group on which to filter." );

        return this.filterByGroup( inGroup, null );
    }

    /**
     * <p>Returns a filtered view that contains the union of thresholds associated with metrics that belong to the 
     * specified group.</p>
     * 
     * <p>See {@link #filterByGroup(MetricConstants.MetricInputGroup, MetricConstants.MetricOutputGroup)} also.</p>
     * 
     * @param outGroup the optional output group
     * @return a filtered view by group
     * @throws NullPointerException if the input is null
     */

    default ThresholdsByMetric filterByGroup( MetricOutputGroup outGroup )
    {
        Objects.requireNonNull( outGroup, "Specify a non-null output group on which to filter." );

        return this.filterByGroup( null, outGroup );
    }

    /**
     * Returns the thresholds associated with a specified type. 
     * 
     * @param type the type of threshold
     * @return the thresholds for a specified type
     * @throws NullPointerException if the input is null 
     */

    Map<MetricConstants, Set<Threshold>> getThresholds( ThresholdType type );

    /**
     * <p>Returns the composed thresholds associated with each metric in the container. A composed threshold is a 
     * {@link OneOrTwoThresholds} that contains two thresholds if the metric consumes 
     * {@link MetricInputGroup#DICHOTOMOUS} and has {@link ThresholdType#PROBABILITY_CLASSIFIER},
     * otherwise one threshold.</p>
     * 
     * <p>Also see: {@link #unionOfOneOrTwoThresholds()}.</p>
     * 
     * @return the composed thresholds
     */

    Map<MetricConstants, Set<OneOrTwoThresholds>> getOneOrTwoThresholds();

    /**
     * Returns <code>true</code> if the store contains thresholds for the specified type, otherwise <code>false</code>.
     * 
     * @param type the type of threshold 
     * @return true if the store contains the type, otherwise false 
     * @throws NullPointerException if the input is null
     */

    boolean hasType( ThresholdType type );

    /**
     * <p>Returns the union of all thresholds in the store. Note that thresholds of type 
     * {@link ThresholdType#PROBABILITY} and {@link ThresholdType#PROBABILITY_CLASSIFIER} may contain exactly the same 
     * parameter values and hence may overlap. In general, this method should not be used for stores that contain 
     * both of these types.</p> 
     * 
     * <p>Also see {@link #unionOfOneOrTwoThresholds()}.</p> 
     * 
     * @return the union of all thresholds
     */

    Set<Threshold> union();

    /**
     * Returns the union of all thresholds returned by {@link #getOneOrTwoThresholds()}.
     * 
     * @return the union of all composed thresholds
     */

    Set<OneOrTwoThresholds> unionOfOneOrTwoThresholds();

    /**
     * Returns the union of all thresholds for a given metric.
     * 
     * @param metric the metric
     * @return the union of all thresholds for the specified metric
     * @throws NullPointerException if the input is null
     */

    Set<Threshold> unionForThisMetric( MetricConstants metric );

    /**
     * Returns the union of all thresholds for one or more types. If no types are specified, returns the empty set.
     * 
     * @param type the type of threshold
     * @return the thresholds for a specified type
     */

    Set<Threshold> unionForTheseTypes( ThresholdType... type );

    /**
     * Returns the union of all thresholds for a given metric and varargs of types. If no types are specified, returns 
     * the empty set.
     * 
     * @param metric the metric
     * @param type the threshold types
     * @return the union of all thresholds for the specified metric and types
     * @throws NullPointerException if the metric is null
     */

    Set<Threshold> unionForThisMetricAndTheseTypes( MetricConstants metric, ThresholdType... type );

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

    ThresholdsByMetric unionWithThisStore( ThresholdsByMetric thresholds );

    /**
     * Returns the set of {@link ThresholdType} in the store.
     * 
     * @return the threshold types stored
     */

    Set<ThresholdType> getThresholdTypes();

    /**
     * Returns the type of thresholds associated with a given metric.
     * 
     * @param metric the metric 
     * @return the type of thresholds 
     * @throws NullPointerException if the input is null
     */

    Set<ThresholdType> getThresholdTypesForThisMetric( MetricConstants metric );

    /**
     * Returns the metrics in the store for which the input threshold is defined.
     * 
     * @param threshold the threshold
     * @return the set of metrics for which the input threshold is defined
     * @throws NullPointerException if the input is null
     */

    Set<MetricConstants> hasTheseMetricsForThisThreshold( Threshold threshold );

    /**
     * Returns the metrics in the store for which the input threshold is not defined.
     * 
     * @param threshold the threshold
     * @return the set of metrics for which the input threshold is not defined
     * @throws NullPointerException if the input is null
     */

    Set<MetricConstants> doesNotHaveTheseMetricsForThisThreshold( Threshold threshold );
    
    /**
     * Returns the set of metrics in the store.
     *
     * @return the stored metrics
     */

    Set<MetricConstants> hasThresholdsForTheseMetrics();
    
    /**
     * Returns a filtered view that contains the union of thresholds for the given input types. If no types are 
     * defined, returns the empty set. If all types are defined, returns this container.
     * 
     * @param type the types
     * @return a filtered view by type
     */

    ThresholdsByMetric filterByType( ThresholdType... type );

    /**
     * Returns a filtered view that contains the union of thresholds associated with metrics that belong to the 
     * specified groups. If both inputs are null, returns the current container.
     * 
     * @param inGroup the optional input group
     * @param outGroup the optional output group
     * @return a filtered view by group
     */

    ThresholdsByMetric filterByGroup( MetricInputGroup inGroup, MetricOutputGroup outGroup );

    /**
     * Builder.
     */

    interface ThresholdsByMetricBuilder
    {

        /**
         * Adds a map of thresholds.
         * 
         * @param thresholds the thresholds
         * @param thresholdType the threshold type
         * @return the builder
         * @throws NullPointerException if any input is null
         */

        ThresholdsByMetricBuilder addThresholds( Map<MetricConstants, Set<Threshold>> thresholds,
                                                 ThresholdType thresholdType );

        /**
         * Builds a {@link ThresholdsByMetric}.
         * 
         * @return the container
         */

        ThresholdsByMetric build();
    }

}
