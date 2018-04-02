package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MissingValues;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricParameterException;

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
public class IndexOfAgreement extends DoubleErrorScore<SingleValuedPairs>
{

    /**
     * Exponent used to calculate the {@link IndexOfAgreement}. 
     */

    final double exponent;

    @Override
    public DoubleScoreOutput apply( final SingleValuedPairs s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }

        double returnMe = MissingValues.MISSING_DOUBLE;

        // Data available
        if ( !s.getData().isEmpty() )
        {
            //Compute the average observation
            double oBar = s.getData().stream().mapToDouble( PairOfDoubles::getItemOne ).average().getAsDouble();
            //Compute the score
            double numerator = 0.0;
            double denominator = 0.0;
            for ( PairOfDoubles nextPair : s.getData() )
            {
                numerator += Math.pow( Math.abs( nextPair.getItemOne() - nextPair.getItemTwo() ), exponent );
                denominator += ( Math.abs( nextPair.getItemTwo() - oBar )
                                 + Math.pow( Math.abs( nextPair.getItemOne() - oBar ), exponent ) );
            }
            returnMe = FunctionFactory.skill().applyAsDouble( numerator, denominator );
        }

        //Metadata
        final MetricOutputMetadata metOut = getMetadata( s, s.getData().size(), MetricConstants.MAIN, null );

        return getDataFactory().ofDoubleScoreOutput( returnMe, metOut );
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
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class IndexOfAgreementBuilder extends DoubleErrorScoreBuilder<SingleValuedPairs>
    {

        @Override
        public IndexOfAgreement build() throws MetricParameterException
        {
            return new IndexOfAgreement( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private IndexOfAgreement( final IndexOfAgreementBuilder builder ) throws MetricParameterException
    {
        super( builder );
        //Default exponent
        exponent = 1.0;
    }

}
