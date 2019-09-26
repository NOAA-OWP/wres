package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MissingValues;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.FunctionFactory;

/**
 * <p>The {@link IndexOfAgreement} was proposed by Willmot (1981) to measure the errors of the model predictions 
 * as a proportion of the degree of variability in the predictions and observations from the average observation. 
 * Originally a quadratic score, different exponents may be used in practice. By default, the absolute errors are 
 * computed with an exponent of one, in order to minimize the influence of extreme errors.</p>  
 * <p>Willmott, C. J. 1981. On the validation of models. <i>Physical Geography</i>, <b>2</b>, 184-194</p>
 * 
 * @author james.brown@hydrosolved.com
 */
public class IndexOfAgreement extends DoubleErrorScore<SampleData<Pair<Double,Double>>>
{

    /**
     * The default exponent.
     */

    private static final double DEFAULT_EXPONENT = 1.0;

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static IndexOfAgreement of()
    {
        return new IndexOfAgreement();
    }

    /**
     * Exponent. 
     */

    final double exponent;

    @Override
    public DoubleScoreStatistic apply( final SampleData<Pair<Double,Double>> s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        double returnMe = MissingValues.MISSING_DOUBLE;

        // Data available
        if ( !s.getRawData().isEmpty() )
        {
            //Compute the average observation
            double oBar = s.getRawData().stream().mapToDouble( Pair::getLeft ).average().getAsDouble();
            //Compute the score
            double numerator = 0.0;
            double denominator = 0.0;
            for ( Pair<Double,Double> nextPair : s.getRawData() )
            {
                numerator += Math.pow( Math.abs( nextPair.getLeft() - nextPair.getRight() ), exponent );
                denominator += ( Math.abs( nextPair.getRight() - oBar )
                                 + Math.pow( Math.abs( nextPair.getLeft() - oBar ), exponent ) );
            }
            returnMe = FunctionFactory.skill().applyAsDouble( numerator, denominator );
        }

        //Metadata
        final StatisticMetadata metOut = StatisticMetadata.of( s.getMetadata(),
                                                                this.getID(),
                                                                MetricConstants.MAIN,
                                                                this.hasRealUnits(),
                                                                s.getRawData().size(),
                                                                null );

        return DoubleScoreStatistic.of( returnMe, metOut );
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.INDEX_OF_AGREEMENT;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * Hidden constructor.
     */

    private IndexOfAgreement()
    {
        super();

        this.exponent = DEFAULT_EXPONENT;
    }

}
