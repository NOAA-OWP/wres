package wres.datamodel.statistics;

import java.util.Map;

import wres.datamodel.MetricConstants;

/**
 * An immutable score statistic that comprises one or more {@link Double} components.
 * 
 * @author james.brown@hydrosolved.com
 */

public class DoubleScoreStatistic extends BasicScoreStatistic<Double, DoubleScoreStatistic>
{

    /**
     * Construct the statistic.
     * 
     * @param statistic the verification statistic
     * @param meta the metadata
     * @throws StatisticException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static DoubleScoreStatistic of( final double statistic, final StatisticMetadata meta )
    {
        return new DoubleScoreStatistic( statistic, meta );
    }

    /**
     * Construct the statistic with a map.
     * 
     * @param statistic the verification output
     * @param meta the metadata
     * @throws StatisticException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static DoubleScoreStatistic of( final Map<MetricConstants, Double> statistic, final StatisticMetadata meta )
    {
        return new DoubleScoreStatistic( statistic, meta );
    }

    @Override
    DoubleScoreStatistic getScore( Double input, StatisticMetadata meta )
    {
        return new DoubleScoreStatistic( input, meta );
    }

    /**
     * Construct the output.
     * 
     * @param statistic the verification statistic
     * @param meta the metadata
     * @throws StatisticException if any of the inputs are invalid
     */

    private DoubleScoreStatistic( final double statistic, final StatisticMetadata meta )
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

    private DoubleScoreStatistic( final Map<MetricConstants, Double> statistic, final StatisticMetadata meta )
    {
        super( statistic, meta );
    }

}
