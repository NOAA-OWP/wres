package wres.engine.statistics.metric.categorical;

import wres.datamodel.MatrixOfDoubles;
import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.pairs.MulticategoryPairs;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MatrixOutput;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * The Peirce Skill Score is a categorical measure of the average accuracy of a predictand for a multi-category event,
 * where the climatological probability of identifying the correct category is factored out. For a dichotomous
 * predictand, the Peirce Skill Score corresponds to the difference between the {@link ProbabilityOfDetection} and the
 * {@link ProbabilityOfFalseDetection}.
 * 
 * @author james.brown@hydrosolved.com
 */
public class PeirceSkillScore<S extends MulticategoryPairs> extends ContingencyTableScore<S>
{

    @Override
    public DoubleScoreOutput apply( final S s )
    {
        return aggregate( this.getInputForAggregation( s ) );
    }

    @Override
    public DoubleScoreOutput aggregate( final MatrixOutput output )
    {
        //Check the input
        this.isContingencyTable( output, this );

        final MatrixOfDoubles v = output.getData();
        final double[][] cm = v.getDoubles();

        //Dichotomous predictand
        if ( v.rows() == 2 )
        {
            double result = FunctionFactory.finiteOrMissing().applyAsDouble( ( cm[0][0] / ( cm[0][0] + cm[1][0] ) )
                                                                         - ( cm[0][1] / ( cm[0][1] + cm[1][1] ) ) );
            return getDataFactory().ofDoubleScoreOutput( result, getMetadata( output ) );
        }

        //Multicategory predictand
        //Compute the sum terms
        double diag = 0.0; //Sum of diagonal
        final double[] rowSums = new double[cm.length]; //Row sums
        final double[] colSums = new double[cm.length]; //Col sums
        for ( int i = 0; i < cm.length; i++ )
        {
            diag += cm[i][i];
            for ( int j = 0; j < cm.length; j++ )
            {
                rowSums[i] += cm[i][j];
                colSums[j] += cm[i][j];
            }
        }
        //Compute the sum product terms
        double sumProd = 0.0;
        double uniProd = 0.0;
        double n = 0.0;
        for ( int i = 0; i < cm.length; i++ )
        {
            sumProd += ( rowSums[i] * colSums[i] );
            uniProd += ( colSums[i] * colSums[i] );
            n += rowSums[i];
        }
        if ( n <= 0 )
        {
            throw new MetricCalculationException( "The sum product of the rows and columns in the contingency table "
                                                  + "must exceed zero when computing the '" + this + "': " + n );
        }
        //Compose the result
        final double nSquared = n * n;
        final double result = FunctionFactory.finiteOrMissing().applyAsDouble( ( ( diag / n ) - ( sumProd / nSquared ) )
                                                                           / ( 1.0 - ( uniProd / nSquared ) ) );
        return getDataFactory().ofDoubleScoreOutput( result, getMetadata( output ) );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.PEIRCE_SKILL_SCORE;
    }

    @Override
    public boolean isSkillScore()
    {
        return true;
    }

    /**
     * A {@link MetricBuilder} to build the dichotomous metric.
     */

    public static class PeirceSkillScoreBuilder<S extends MulticategoryPairs>
            extends OrdinaryScoreBuilder<S, DoubleScoreOutput>
    {

        @Override
        public PeirceSkillScore<S> build() throws MetricParameterException
        {
            return new PeirceSkillScore<>( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    private PeirceSkillScore( final PeirceSkillScoreBuilder<S> builder ) throws MetricParameterException
    {
        super( builder );
    }

}
