package wres.io.retrieval;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import wres.config.generated.TimeScaleFunction;
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
class CondensedIngestedValue
{
    /**
     * The date at which the condensed values will represent
     */
    final Instant validTime;

    /**
     * The final lead for the condensed set of values
     */
    final int lead;

    /**
     * The collection of values mapped to their groups
     * <br><br>
     * The groups are generally separate ensembles
     */
    private final Map<Integer, List<Double>> valueMapping;

    /**
     * Creates a new Condensed set of Ingested Values
     * @param validTime The time at which the final set of values will be valid
     * @param lead The lead at which the final set of values are valid
     * @param valueMapping The collection of values that will be condensed
     */
    CondensedIngestedValue (final Instant validTime, final int lead, final Map<Integer, List<Double>> valueMapping)
    {
        this.validTime = validTime;
        this.lead = lead;
        this.valueMapping = java.util.Collections.unmodifiableMap(valueMapping);
    }

    /**
     * Shrinks down all mapped values into a single array
     * @param scale Whether or not the values should be scaled
     * @param function The function used to scale the values
     * @return An array of condensed values
     */
    Double[] getAggregatedValues( Boolean scale, TimeScaleFunction function )
    {
        List<Double> aggregatedValues = new ArrayList<>(  );
        for (List<Double> values : valueMapping.values())
        {
            if (scale)
            {
                aggregatedValues.add( Collections.aggregate(values, function.value()));
            }
            else
            {
                aggregatedValues.addAll( values );
            }
        }

        return aggregatedValues.toArray( new Double[aggregatedValues.size()] );
    }

    /**
     * @return Whether or not the collection of values are empty
     */
    boolean isEmpty()
    {
        return valueMapping.isEmpty();
    }
}
