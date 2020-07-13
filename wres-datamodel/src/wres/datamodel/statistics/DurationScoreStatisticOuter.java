package wres.datamodel.statistics;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import net.jcip.annotations.Immutable;
import wres.datamodel.MetricConstants;
import wres.datamodel.messages.MessageFactory;
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
     * Construct the statistic.
     * 
     * @param statistic the verification statistic
     * @param metadata the metadata
     * @throws StatisticException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static DurationScoreStatisticOuter of( DurationScoreStatistic statistic, StatisticMetadata metadata )
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
         * Hidden constructor.
         * @param name the name
         * @param component the score component
         * @param metadata the metadata
         */

        private DurationScoreComponentOuter( MetricConstants name,
                                             DurationScoreStatisticComponent component,
                                             StatisticMetadata metadata )
        {
            super( name, component, metadata, next -> MessageFactory.parse( next.getValue() ).toString() );
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

        public static DurationScoreComponentOuter of( MetricConstants name,
                                                      DurationScoreStatisticComponent component,
                                                      StatisticMetadata metadata )
        {
            return new DurationScoreComponentOuter( name, component, metadata );
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param score the score
     */

    private DurationScoreStatisticOuter( DurationScoreStatistic score, StatisticMetadata metadata )
    {
        super( score, DurationScoreStatisticOuter.createInternalMapping( score, metadata ), metadata );
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
                                   StatisticMetadata metadata )
    {
        Map<MetricConstants, DurationScoreComponentOuter> returnMe = new EnumMap<>( MetricConstants.class );

        List<DurationScoreStatisticComponent> components = score.getStatisticsList();
        for ( DurationScoreStatisticComponent next : components )
        {
            MetricConstants name = MetricConstants.valueOf( next.getName().name() );
            DurationScoreComponentOuter nextOuter = DurationScoreComponentOuter.of( name, next, metadata );

            returnMe.put( name, nextOuter );
        }

        return Collections.unmodifiableMap( returnMe );
    }

}
