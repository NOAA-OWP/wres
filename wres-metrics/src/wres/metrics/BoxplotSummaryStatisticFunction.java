package wres.metrics;

import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.SummaryStatistic;

/**
 * A box plot summary statistic that wraps a {@link SummaryStatistic} and provides behavior.
 *
 * @param statistic the statistic, not null
 * @param calculator the statistic calculator, not null
 * @author James Brown
 */
public record BoxplotSummaryStatisticFunction( SummaryStatistic statistic,
                                               BiFunction<Map<SummaryStatisticComponentName, String>, double[], BoxplotStatistic> calculator )
        implements BiFunction<Map<SummaryStatisticComponentName, String>, double[], BoxplotStatistic>
{
    /**
     * @param statistic the statistic
     * @param calculator the calculator
     */
    public BoxplotSummaryStatisticFunction
    {
        Objects.requireNonNull( statistic );
        Objects.requireNonNull( calculator );
    }

    @Override
    public BoxplotStatistic apply( Map<SummaryStatisticComponentName,String> parameters, double[] value )
    {
        return this.calculator.apply( parameters, value );
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "statistic", this.statistic()
                                          .getStatistic() )
                .append( "dimension", this.statistic()
                                          .getDimension() )
                .toString();
    }
}
