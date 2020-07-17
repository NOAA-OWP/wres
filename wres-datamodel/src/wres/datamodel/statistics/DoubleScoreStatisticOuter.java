package wres.datamodel.statistics;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import net.jcip.annotations.Immutable;
import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.datamodel.statistics.BasicScoreStatistic.BasicScoreComponent;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * An immutable score statistic that wraps a {@link DoubleScoreStatistic}.
 * 
 * @author james.brown@hydrosolved.com
 */

@Immutable
public class DoubleScoreStatisticOuter extends BasicScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter>
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

    public static DoubleScoreStatisticOuter of( DoubleScoreStatistic statistic, SampleMetadata metadata )
    {
        return new DoubleScoreStatisticOuter( statistic, metadata );
    }

    /**
     * A wrapper for a {@link DoubleScoreStatisticComponent}.
     * 
     * @author james.brown@hydrosolved.com
     */

    public static class DoubleScoreComponentOuter extends BasicScoreComponent<DoubleScoreStatisticComponent>
    {

        /**
         * The component name.
         */

        private final MetricConstants metricName;

        /**
         * Hidden constructor.
         * @param name the name
         * @param component the score component
         * @param metadata the metadata
         */

        private DoubleScoreComponentOuter( MetricConstants name,
                                           DoubleScoreStatisticComponent component,
                                           SampleMetadata metadata )
        {
            super( name, component, metadata, next -> Double.toString( next.getValue() ) );

            this.metricName = MetricConstants.valueOf( component.getName().name() );
        }

        /**
         * Create a component.
         * 
         * @param name the name
         * @param component the score component
         * @param metadata the metadata
         * @return a component
         * @throws NullPointerException if any input is null 
         */

        public static DoubleScoreComponentOuter of( MetricConstants name,
                                                    DoubleScoreStatisticComponent component,
                                                    SampleMetadata metadata )
        {
            return new DoubleScoreComponentOuter( name, component, metadata );
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

        builder.append( "metadata", this.getMetadata() )
               .append( "sample size", this.getData().getSampleSize() );

        return builder.toString();
    }

    /**
     * Hidden constructor.
     * 
     * @param score the score
     */

    private DoubleScoreStatisticOuter( DoubleScoreStatistic score, SampleMetadata metadata )
    {
        super( score, DoubleScoreStatisticOuter.createInternalMapping( score, metadata ), metadata );

        this.metricName = MetricConstants.valueOf( score.getMetric().getName().name() );
    }

    /**
     * Creates an internal mapping between score components and their identifiers.
     * 
     * @param score the score
     * @param metadata the metadata
     * @return the internal mapping for re-use
     */

    private static Map<MetricConstants, DoubleScoreComponentOuter> createInternalMapping( DoubleScoreStatistic score,
                                                                                          SampleMetadata metadata )
    {
        Map<MetricConstants, DoubleScoreComponentOuter> returnMe = new EnumMap<>( MetricConstants.class );

        List<DoubleScoreStatisticComponent> components = score.getStatisticsList();
        for ( DoubleScoreStatisticComponent next : components )
        {
            MetricConstants name = MetricConstants.valueOf( next.getName().name() );
            DoubleScoreComponentOuter nextOuter = DoubleScoreComponentOuter.of( name, next, metadata );

            returnMe.put( name, nextOuter );
        }

        return Collections.unmodifiableMap( returnMe );
    }
}
