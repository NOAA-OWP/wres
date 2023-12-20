package wres.metrics;

import java.util.Objects;
import java.util.function.ToDoubleFunction;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import wres.statistics.generated.SummaryStatistic;

/**
 * A univariate summary statistic that wraps a {@link SummaryStatistic} and provides behavior.
 *
 * @param statistic the statistic, not null
 * @param calculator the statistic calculator, not null
 * @author James Brown
 */
public record ScalarSummaryStatisticFunction( SummaryStatistic statistic,
                                              ToDoubleFunction<double[]> calculator ) implements ToDoubleFunction<double[]>
{
    /**
     * @param statistic the statistic
     * @param calculator the calculator
     */
    public ScalarSummaryStatisticFunction
    {
        Objects.requireNonNull( statistic );
        Objects.requireNonNull( calculator );
    }

    @Override
    public double applyAsDouble( double[] value )
    {
        return this.calculator.applyAsDouble( value );
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "statistic", this.statistic()
                                          .getStatistic() )
                .append( "dimension", this.statistic()
                                          .getDimension() )
                .append( "probability", this.statistic()
                                            .getProbability() )
                .toString();
    }
}
