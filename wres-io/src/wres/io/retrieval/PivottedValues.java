package wres.io.retrieval;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.generated.TimeScaleFunction;
import wres.io.data.caching.Ensembles;
import wres.io.data.details.EnsembleDetails;
import wres.util.Collections;

/**
 * A collection of grouped values that will be aggregated into a collection of values used for pairing
 * <br><br>
 * The collection will turn the values:
 * <br><br>
 * group 1: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]<br>
 * group 2: [7.0, 8.0, 9.0, 10.0, 11.0, 12.0]<br>
 * group 3: [13.0, 14.0, 15.0, 16.0, 17.0, 18.0]<br>
 * group 4: [19.0, 20.0, 21.0, 22.0, 23.0, 24.0]<br>
 * <br><br>
 * Into:
 * <br><br>
 * [agg(group 1), agg(group 2), agg(group 3), agg(group 4)]
 * <br><br>
 * The groups are generally different ensembles
 */
class PivottedValues
{
    /**
     * The date at which the condensed values will represent
     */
    private final Instant validTime;

    /**
     * The final lead for the condensed set of values
     */
    private final Duration lead;

    /**
     * The collection of values mapped to the position in the arrays they came in as and the id of their ensemble
     */
    private final Map<EnsemblePosition, List<Double>> valueMapping;

    /**
     * Creates a new Condensed set of Ingested Values
     * @param validTime The time at which the final set of values will be valid
     * @param lead The lead at which the final set of values are valid
     * @param valueMapping The collection of values that will be condensed
     */
    PivottedValues( final Instant validTime, final Duration lead, final Map<EnsemblePosition, List<Double>> valueMapping)
    {
        this.validTime = validTime;
        this.lead = lead;
        this.valueMapping = java.util.Collections.unmodifiableMap(valueMapping);
    }

    /**
     * @return Ensemble metadata for each element in sorted order
     */
    Collection<EnsembleDetails> getEnsembleMembers()
    {
        List<Integer> ensembleIDs = new ArrayList<>();
        this.valueMapping.keySet().forEach( ensemblePosition -> ensembleIDs.add( ensemblePosition.ensembleId ) );

        return Ensembles.getEnsembleDetails(ensembleIDs);
    }

    /**
     * @return the lead duration
     */
    
    Duration getLeadDuration()
    {
        return this.lead;
    }
    
    /**
     * @return the valid datetime.
     */
    
    Instant getValidTime()
    {
        return this.validTime;
    }
    
    /**
     * Shrinks down all mapped values into a single array
     * @param scale Whether or not the values should be scaled
     * @param function The function used to scale the values
     * @return An array of condensed values
     */
    Double[] getAggregatedValues( Boolean scale, TimeScaleFunction function )
    {
        List<Pair<Integer, Double>> aggregatedValues = new ArrayList<>(  );
        for (Entry<EnsemblePosition, List<Double>> values : valueMapping.entrySet())
        {
            if (scale)
            {
                // If we're scaling, we want to combine the values and add the result to the list for sorting
                aggregatedValues.add(
                        Pair.of(values.getKey().ensembleId,
                                Collections.upscale(values.getValue(), function.value()))
                );
            }
            else
            {
                // Otherwise we just want to add each value from the list
                for (Double value : values.getValue())
                {
                    aggregatedValues.add( Pair.of( values.getKey().ensembleId, value ) );
                }
            }
        }

        // Sort the values in member label order
        Collection<Double> sortedValues = this.sortAggregatedValues( aggregatedValues );
        return sortedValues.toArray( new Double[0] );
    }

    /**
     * Creates a collection of values sorted by the labels corresponding to the ensemble ids of the values in aggregatedValues
     * @param aggregatedValues A collection of values paired to their ensemble id
     * @return A collection of the values sorted in ensemble label order
     */
    private Collection<Double> sortAggregatedValues(final List<Pair<Integer, Double>> aggregatedValues)
    {
        List<Double> sortedAggregatedValues = new ArrayList<>();

        // Get the collection of ensembles in label order
        for (EnsembleDetails member : this.getEnsembleMembers())
        {
            Pair<Integer, Double> correspondingMemberValue = null;

            // Find the first value in the passed in list with a matching ensemble id
            for (Pair<Integer, Double> memberValue : aggregatedValues)
            {
                if (memberValue.getLeft().equals(member.getId()))
                {
                    correspondingMemberValue = memberValue;
                    break;
                }
            }

            Objects.requireNonNull(correspondingMemberValue,
                                   "The value for the ensemble member '" + member + "' could not be found.");

            // Remove the found value pair from the passed in list so that the value isn't retrieved again
            aggregatedValues.remove( correspondingMemberValue );

            // Add the value to the collection that will be returned
            sortedAggregatedValues.add( correspondingMemberValue.getValue() );
        }

        return sortedAggregatedValues;
    }

    /**
     * @return Whether or not the collection of values are empty
     */
    boolean isEmpty()
    {
        return valueMapping.isEmpty();
    }

    /**
     * A key class combining a) the position of an ensemble id in a returned array and the value of the ensemble id
     */
    public static class EnsemblePosition implements Comparable<EnsemblePosition>
    {
        /**
         * The position of the pivotted value in the array that loaded the information to pivot
         */
        private final int positionId;

        /**
         * The id for the member at the given position in the pivotted data
         */
        private final int ensembleId;

        /**
         * Constructor
         * @param positionId The position of a pivotted value
         * @param ensembleId The id of the member that the pivotted value belongs to
         */
        EnsemblePosition(final int positionId, final int ensembleId)
        {
            this.positionId = positionId;
            this.ensembleId = ensembleId;
        }

        @Override
        public boolean equals( Object obj )
        {
            if (obj instanceof EnsemblePosition)
            {
                EnsemblePosition other = (EnsemblePosition)obj;
                return this.positionId == other.positionId && this.ensembleId == other.positionId;
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( this.positionId, this.ensembleId );
        }

        /**
         * Sort by first the position, then the id of the member
         * @param ensemblePosition The EnsemblePosition to compare to
         * @return -1 if this instance has a lower position than ensemblePosition,
         * 0 if they have the same position id and ensemble id,
         * and 1 if this instance has a higher position than ensembleMember
         */
        @Override
        public int compareTo( EnsemblePosition ensemblePosition )
        {
            int order = Integer.compare( this.positionId, ensemblePosition.positionId );

            if (order == 0)
            {
                return Integer.compare( this.ensembleId, ensemblePosition.ensembleId );
            }

            return order;
        }
    }
}
