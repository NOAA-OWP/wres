package wres.io.retrieval;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import wres.config.generated.TimeScaleFunction;
import wres.util.Collections;

class CondensedIngestedValue
{
    final Instant validTime;
    final int lead;
    final Map<Integer, List<Double>> valueMapping;

    CondensedIngestedValue (final Instant validTime, final int lead, final Map<Integer, List<Double>> valueMapping)
    {
        this.validTime = validTime;
        this.lead = lead;
        this.valueMapping = java.util.Collections.unmodifiableMap(valueMapping);
    }

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

    boolean isEmpty()
    {
        return valueMapping.isEmpty();
    }
}
