package wres.engine.statistics.metric;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.DoubleBinaryOperator;
import java.util.function.ToDoubleFunction;

import org.apache.commons.math3.stat.descriptive.AbstractUnivariateStatistic;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.stat.descriptive.rank.Max;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.apache.commons.math3.stat.descriptive.rank.Min;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubles;

/**
 * A factory class for constructing elementary functions.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class FunctionFactory
{

    /**
     * Map of summary statistics.
     */

    private static final Map<MetricConstants, AbstractUnivariateStatistic> STATISTICS = new HashMap<>();
       
    /**
     * Return a function that computes the difference between the second and first entries in a {@link PairOfDoubles}.
     * 
     * @return a function that computes the error
     */

    public static DoubleErrorFunction error()
    {
        return a -> a.getItemTwo() - a.getItemOne();
    }

    /**
     * Return a function that computes the absolute difference between the first and second entries in a
     * {@link PairOfDoubles}.
     * 
     * @return a function that computes the absolute error
     */

    public static DoubleErrorFunction absError()
    {
        return a -> Math.abs( a.getItemOne() - a.getItemTwo() );
    }

    /**
     * Return a function that computes the square difference between the first and second entries in a
     * {@link PairOfDoubles}.
     * 
     * @return a function that computes the square error
     */

    public static DoubleErrorFunction squareError()
    {
        return a -> Math.pow( a.getItemOne() - a.getItemTwo(), 2 );
    }

    /**
     * <p>
     * Return a function that computes a skill score from two elementary scores:
     * </p>
     * <p>
     * <code>(a,b) -&gt; 1.0 - (a / b)</code>
     * </p>
     * 
     * @return a function that computes the skill
     */

    public static DoubleBinaryOperator skill()
    {
        return ( a, b ) -> 1.0 - ( a / b );
    }

    /**
     * Rounds the input to the prescribed number of decimal places using {@link BigDecimal#ROUND_HALF_UP}.
     * 
     * @return a function that rounds to a prescribed number of decimal places
     */

    public static BiFunction<Double, Integer, Double> round()
    {
        return ( input, digits ) -> {
            BigDecimal bd = new BigDecimal( Double.toString( input ) ); //Always use String constructor
            bd = bd.setScale( digits, BigDecimal.ROUND_HALF_UP );
            return bd.doubleValue();
        };
    }

    /**
     * <p>
     * Return a function that computes the equality of two doubles to 8 d.p.
     * </p>
     * 
     * @return a function that computes the skill
     */

    public static BiPredicate<Double, Double> doubleEquals()
    {
        return ( a, b ) -> Math.abs( a - b ) < .00000001;
    }

    /**
     * <p>
     * Return a function that computes the mean average of a vector of doubles.
     * </p>
     * 
     * @return a function that computes the mean
     */

    public static ToDoubleFunction<VectorOfDoubles> mean()
    {
        return a -> Arrays.stream( a.getDoubles() ).average().getAsDouble();
    }   

    /**
     * <p>
     * Return a function that computes the sample standard deviation of a vector of doubles.
     * </p>
     * 
     * @return a function that computes the standard deviation
     */

    public static ToDoubleFunction<VectorOfDoubles> standardDeviation()
    {
        return a -> {
            double mean = mean().applyAsDouble( a );
            return Math.sqrt( Arrays.stream( a.getDoubles() )
                                    .map( d -> Math.pow( d - mean, 2 ) )
                                    .sum()
                              / (a.size() - 1.0) );
        };
    }
    
    /**
     * Returns a statistic associated with a {@link MetricConstants} that belongs to the 
     * {@link ScoreOutputGroup#UNIVARIATE_STATISTIC}.
     * 
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the input does not belong to {@link ScoreOutputGroup#UNIVARIATE_STATISTIC} 
     *            or the statistic does not exist
     */
    
    public static AbstractUnivariateStatistic ofStatistic( MetricConstants statistic )
    {
        Objects.requireNonNull( statistic );
        if( ! statistic.isInGroup( ScoreOutputGroup.UNIVARIATE_STATISTIC ) )
        {
            throw new IllegalArgumentException( "The statistic '"+statistic+"' is not a recognized statistic "
                    + "in this context." );
        }
        // Lazy build the map
        buildStatisticsMap();
        if( ! STATISTICS.containsKey( statistic ) )
        {
            throw new IllegalArgumentException( "The statistic '"+statistic+"' has not been implemented." );
        }
        return STATISTICS.get( statistic );
    }
    

    /**
     * No argument constructor.
     */

    private FunctionFactory()
    {
    };
    
    /**
     * Builds the map of statistics.
     */

    private static void buildStatisticsMap()
    {
        STATISTICS.put( MetricConstants.MEAN, new Mean() );
        STATISTICS.put( MetricConstants.MEDIAN, new Median() );
        STATISTICS.put( MetricConstants.STANDARD_DEVIATION, new StandardDeviation() );
        STATISTICS.put( MetricConstants.MINIMUM, new Min() );
        STATISTICS.put( MetricConstants.MAXIMUM, new Max() );       
    }
    
}
