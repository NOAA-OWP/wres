package wres.metrics;

import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.SummaryStatistic;

/**
 * A diagram summary statistic that wraps a {@link SummaryStatistic} and provides behavior.
 *
 * @param statistic the statistic, not null
 * @param calculator the statistic calculator, not null
 * @author James Brown
 */
public record DiagramSummaryStatisticFunction( SummaryStatistic statistic,
                                               BiFunction<Map<SummaryStatisticComponentName, String>, double[], DiagramStatistic> calculator )
        implements BiFunction<Map<SummaryStatisticComponentName, String>, double[], DiagramStatistic>
{
    /**
     * @param statistic the statistic
     * @param calculator the calculator
     */
    public DiagramSummaryStatisticFunction
    {
        Objects.requireNonNull( statistic );
        Objects.requireNonNull( calculator );
    }

    @Override
    public DiagramStatistic apply( Map<SummaryStatisticComponentName, String> names, double[] value )
    {
        return this.calculator.apply( names, value );
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "statistic", this.statistic()
                                          .getStatistic() )
                .append( "dimensions", this.statistic()
                                           .getDimensionList() )
                .toString();
    }
}
