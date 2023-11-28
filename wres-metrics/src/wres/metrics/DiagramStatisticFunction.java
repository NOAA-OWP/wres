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
public record DiagramStatisticFunction<T>( SummaryStatistic statistic,
                                           BiFunction<Map<DiagramComponentName, String>, T, DiagramStatistic> calculator )
        implements BiFunction<Map<DiagramStatisticFunction.DiagramComponentName, String>, T, DiagramStatistic>
{
    /**
     * An enumeration of diagram component names to be associated with string values.
     * @author James Brown
     */
    public enum DiagramComponentName
    {
        /** The variable to which the diagram refers. */
        VARIABLE,
        /** The unit of the variable. */
        VARIABLE_UNIT
    }

    /**
     * @param statistic the statistic
     * @param calculator the calculator
     */
    public DiagramStatisticFunction
    {
        Objects.requireNonNull( statistic );
        Objects.requireNonNull( calculator );
    }

    @Override
    public DiagramStatistic apply( Map<DiagramComponentName, String> names, T value )
    {
        return this.calculator.apply( names, value );
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
