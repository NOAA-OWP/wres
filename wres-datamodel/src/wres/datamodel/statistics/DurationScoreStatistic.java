package wres.datamodel.statistics;

import java.time.Duration;
import java.util.Map;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreGroup;

/**
 * An immutable statistic that comprises one or more {@link Duration} components.
 * 
 * @author james.brown@hydrosolved.com
 */

public class DurationScoreStatistic extends BasicScoreStatistic<Duration, DurationScoreStatistic>
{

    /**
     * Construct the statistic.
     * 
     * @param statistic the verification statistic
     * @param meta the metadata
     * @throws StatisticException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static DurationScoreStatistic of( final Duration statistic, final StatisticMetadata meta )
    {
        return new DurationScoreStatistic( statistic, meta );
    }

    /**
     * Construct the statistic with a map.
     * 
     * @param statistic the verification statistic
     * @param meta the metadata
     * @throws StatisticException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static DurationScoreStatistic of( final Map<MetricConstants, Duration> statistic,
                                             final StatisticMetadata meta )
    {
        return new DurationScoreStatistic( statistic, meta );
    }

    /**
     * Construct the output with a template.
     * 
     * @param statistic the verification output
     * @param template the score template
     * @param meta the metadata
     * @throws StatisticException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static DurationScoreStatistic
            of( final Duration[] statistic, final ScoreGroup template, final StatisticMetadata meta )
    {
        return new DurationScoreStatistic( statistic, template, meta );
    }

    @Override
    DurationScoreStatistic getScore( Duration input, StatisticMetadata meta )
    {
        return new DurationScoreStatistic( input, meta );
    }

    /**
     * Construct the statistic.
     * 
     * @param statistic the verification statistic
     * @param meta the metadata
     * @throws StatisticException if any of the inputs are invalid
     */

    private DurationScoreStatistic( final Duration statistic, final StatisticMetadata meta )
    {
        super( statistic, meta );
    }

    /**
     * Construct the statistic with a map.
     * 
     * @param statistic the verification statistic
     * @param meta the metadata
     * @throws StatisticException if any of the inputs are invalid
     */

    private DurationScoreStatistic( final Map<MetricConstants, Duration> statistic, final StatisticMetadata meta )
    {
        super( statistic, meta );
    }

    /**
     * Construct the statistic with a template.
     * 
     * @param statistic the verification statistic
     * @param template the score template
     * @param meta the metadata
     * @throws StatisticException if any of the inputs are invalid
     */

    private DurationScoreStatistic( final Duration[] statistic,
                                    final ScoreGroup template,
                                    final StatisticMetadata meta )
    {
        super( statistic, template, meta );
    }

}
