package wres.writing.netcdf;

import java.util.Comparator;
import org.jetbrains.annotations.NotNull;
import wres.config.MetricConstants;

/**
 * A metric name, containing the overall metric, the component, where applicable, and a composite string identifier.
 * @param metric the metric
 * @param component the metric component
 * @param metricName the metric name
 * @author James Brown
 */

record MetricNames( MetricConstants metric,
                    MetricConstants component,
                    String metricName ) implements Comparable<MetricNames>
{
    /** Null safe enum comparator. */
    private static final Comparator<MetricConstants> ENUM_COMPARE = Comparator.nullsFirst( Enum::compareTo );
    /** Null safe string comparator. */
    private static final Comparator<String> STRING_COMPARE = Comparator.nullsFirst( String::compareTo );

    @Override
    public int compareTo( @NotNull MetricNames o )
    {
        int result = this.metric.compareTo( o.metric );

        if ( result != 0 )
        {
            return result;
        }

        result = ENUM_COMPARE.compare( this.component, o.component );

        if ( result != 0 )
        {
            return result;
        }

        return STRING_COMPARE.compare( this.metricName, o.metricName );
    }
}
