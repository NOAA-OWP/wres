package wres.datamodel.statistics;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import net.jcip.annotations.Immutable;
import wres.datamodel.MetricConstants;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.datamodel.statistics.BasicScoreStatistic.BasicScoreComponent;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * An immutable score statistic that comprises one or more {@link Double} components.
 * 
 * @author james.brown@hydrosolved.com
 */

@Immutable
public class DoubleScoreStatisticOuter extends BasicScoreStatistic<DoubleScoreStatistic, DoubleScoreComponentOuter>
{

    /**
     * Construct the statistic.
     * 
     * @param statistic the verification statistic
     * @param metadata the metadata
     * @throws StatisticException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static DoubleScoreStatisticOuter of( DoubleScoreStatistic statistic, StatisticMetadata metadata )
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
         * Hidden constructor.
         * @param name the name
         * @param component the score component
         * @param metadata the metadata
         */

        private DoubleScoreComponentOuter( MetricConstants name,
                                           DoubleScoreStatisticComponent component,
                                           StatisticMetadata metadata )
        {
            super( name, component, metadata, next -> Double.toString( next.getValue() ) );
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
                                             StatisticMetadata metadata )
        {
            return new DoubleScoreComponentOuter( name, component, metadata );
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param score the score
     */

    private DoubleScoreStatisticOuter( DoubleScoreStatistic score, StatisticMetadata metadata )
    {
        super( score, DoubleScoreStatisticOuter.createInternalMapping( score, metadata ), metadata );
    }

    /**
     * Creates an internal mapping between score components and their identifiers.
     * 
     * @param score the score
     * @param metadata the metadata
     * @return the internal mapping for re-use
     */

    private static Map<MetricConstants, DoubleScoreComponentOuter> createInternalMapping( DoubleScoreStatistic score,
                                                                                          StatisticMetadata metadata )
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
