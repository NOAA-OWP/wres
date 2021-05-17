package wres.datamodel.statistics;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import net.jcip.annotations.Immutable;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DurationScoreStatisticOuter.DurationScoreComponentOuter;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.DurationScoreStatistic.DurationScoreStatisticComponent;
import wres.datamodel.statistics.BasicScoreStatistic.BasicScoreComponent;

/**
 * An immutable score statistic that wraps a {@link DurationScoreStatistic}.
 * 
 * @author james.brown@hydrosolved.com
 */

@Immutable
public class DurationScoreStatisticOuter
        extends BasicScoreStatistic<DurationScoreStatistic, DurationScoreComponentOuter>
{

    /**
     * The metric name.
     */

    private final MetricConstants metricName;

    /**
     * Construct the statistic.
     * 
     * @param statistic the verification statistic
     * @param metadata the metadata
     * @throws StatisticException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static DurationScoreStatisticOuter of( DurationScoreStatistic statistic, PoolMetadata metadata )
    {
        return new DurationScoreStatisticOuter( statistic, metadata );
    }

    /**
     * A wrapper for a {@link DurationScoreStatisticComponent}.
     * 
     * @author james.brown@hydrosolved.com
     */

    public static class DurationScoreComponentOuter extends BasicScoreComponent<DurationScoreStatisticComponent>
    {

        /**
         * The component name.
         */

        private final MetricConstants metricName;

        /**
         * Hidden constructor.
         * @param component the score component
         * @param metadata the metadata
         */

        private DurationScoreComponentOuter( DurationScoreStatisticComponent component,
                                             PoolMetadata metadata )
        {
            super( component, metadata, next -> MessageFactory.parse( next.getValue() ).toString() );

            this.metricName = MetricConstants.valueOf( component.getMetric().getName().name() );
        }

        /**
         * Create a component.
         * 
         * @param component the score component
         * @param metadata the metadata
         * @return a component
         * @throws NullPointerException if any input is null 
         */

        public static DurationScoreComponentOuter of( DurationScoreStatisticComponent component,
                                                      PoolMetadata metadata )
        {
            return new DurationScoreComponentOuter( component, metadata );
        }

        @Override
        public MetricConstants getMetricName()
        {
            return this.metricName;
        }
    }

    @Override
    public MetricConstants getMetricName()
    {
        return this.metricName;
    }

    @Override
    public String toString()
    {
        ToStringBuilder builder = new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE );

        builder.append( "metric", this.getData().getMetric().getName() );

        this.getInternalMapping().forEach( ( key, value ) -> builder.append( "value", value ) );

        builder.append( "metadata", this.getMetadata() );

        return builder.toString();
    }

    /**
     * Hidden constructor.
     * 
     * @param score the score
     */

    private DurationScoreStatisticOuter( DurationScoreStatistic score, PoolMetadata metadata )
    {
        super( score, DurationScoreStatisticOuter.createInternalMapping( score, metadata ), metadata );

        this.metricName = MetricConstants.valueOf( score.getMetric().getName().name() );
    }

    /**
     * Creates an internal mapping between score components and their identifiers.
     * 
     * @param score the score
     * @param metadata the metadata
     * @return the internal mapping for re-use
     */

    private static Map<MetricConstants, DurationScoreComponentOuter>
            createInternalMapping( DurationScoreStatistic score,
                                   PoolMetadata metadata )
    {
        Map<MetricConstants, DurationScoreComponentOuter> returnMe = new EnumMap<>( MetricConstants.class );

        List<DurationScoreStatisticComponent> components = score.getStatisticsList();
        for ( DurationScoreStatisticComponent next : components )
        {
            MetricConstants name = MetricConstants.valueOf( next.getMetric().getName().name() );
            DurationScoreComponentOuter nextOuter = DurationScoreComponentOuter.of( next, metadata );

            returnMe.put( name, nextOuter );
        }

        return Collections.unmodifiableMap( returnMe );
    }

}
