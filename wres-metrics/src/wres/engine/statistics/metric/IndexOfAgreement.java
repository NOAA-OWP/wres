package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDecompositionGroup;
import wres.datamodel.MetricOutputMetadata;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.ScalarOutput;
import wres.datamodel.SingleValuedPairs;

/**
 * <p>The {@link IndexOfAgreement} was proposed by Willmot (1981) to measure the errors of the model predictions 
 * as a proportion of the degree of variability in the predictions and observations from the average observation. 
 * Originally a quadratic score, different exponents may be used in practice. By default, the absolute errors are 
 * computed with an exponent of one, in order to minimize the influence of extreme errors.</p>  
 * <p>Willmott, C. J. 1981. On the validation of models. <i>Physical Geography</i>, <b>2</b>, 184-194</p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class IndexOfAgreement extends DoubleErrorScore<SingleValuedPairs>
{

    /**
     * Exponent used to calculate the {@link IndexOfAgreement}. 
     */

    final double exponent;

    @Override
    public ScalarOutput apply( final SingleValuedPairs s )
    {
        Objects.requireNonNull( s, "Specify non-null input for the '" + toString() + "'." );
        //Compute the average observation
        double oBar = s.getData().stream().mapToDouble( a -> a.getItemOne() ).average().getAsDouble();
        //Compute the score
        double numerator = 0.0;
        double denominator = 0.0;
        for ( PairOfDoubles nextPair : s.getData() )
        {
            numerator += Math.pow( Math.abs( nextPair.getItemOne() - nextPair.getItemTwo() ), exponent );
            denominator += ( Math.abs( nextPair.getItemTwo() - oBar )
                             + Math.pow( Math.abs( nextPair.getItemOne() - oBar ), exponent ) );
        }

        //Metadata
        final MetricOutputMetadata metOut = getMetadata( s, s.getData().size(), MetricConstants.MAIN, null );
        //Compute the atomic errors in a stream
        return getDataFactory().ofScalarOutput( FunctionFactory.skill().applyAsDouble( numerator, denominator ),
                                                metOut );
    }

    @Override
    public boolean isSkillScore()
    {
        return true;
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.INDEX_OF_AGREEMENT;
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    @Override
    public MetricDecompositionGroup getDecompositionID()
    {
        return MetricDecompositionGroup.NONE;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    static class IndexOfAgreementBuilder extends DoubleErrorScoreBuilder<SingleValuedPairs>
    {

        @Override
        protected IndexOfAgreement build()
        {
            return new IndexOfAgreement( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private IndexOfAgreement( final IndexOfAgreementBuilder builder )
    {
        super( builder );
        //Default exponent
        exponent = 1.0;
    }

}
