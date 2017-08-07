package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.MatrixOfDoubles;
import wres.datamodel.metric.MatrixOutput;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MulticategoryPairs;
import wres.datamodel.metric.ScalarOutput;

/**
 * The Peirce Skill Score is a categorical measure of the average accuracy of a predictand for a multi-category event,
 * where the climatological probability of identifying the correct category is factored out. For a dichotomous
 * predictand, the Peirce Skill Score corresponds to the difference between the {@link ProbabilityOfDetection} and the
 * {@link ProbabilityOfFalseDetection}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class PeirceSkillScore<S extends MulticategoryPairs> extends ContingencyTableScore<S>
{

    @Override
    public ScalarOutput apply(final S s)
    {
        return apply(getCollectionInput(s));
    }

    @Override
    public ScalarOutput apply(final MatrixOutput output)
    {
        Objects.requireNonNull(output, "Specify non-null input for the '" + toString() + "'.");
        //Check the input
        isContingencyTable(output, this);

        final MatrixOfDoubles v = output.getData();
        final double[][] cm = v.getDoubles();

        //Dichotomous predictand
        if(v.rows() == 2)
        {
            return getDataFactory().ofScalarOutput((cm[0][0] / (cm[0][0] + cm[1][0]))
                - (cm[0][1] / (cm[0][1] + cm[1][1])), getMetadata(output));
        }

        //Multicategory predictand
        //Compute the sum terms
        double diag = 0.0; //Sum of diagonal
        final double[] rowSums = new double[cm.length]; //Row sums
        final double[] colSums = new double[cm.length]; //Col sums
        for(int i = 0; i < cm.length; i++)
        {
            diag += cm[i][i];
            for(int j = 0; j < cm.length; j++)
            {
                rowSums[i] += cm[i][j];
                colSums[j] += cm[i][j];
            }
        }
        //Compute the sum product terms
        double sumProd = 0.0;
        double uniProd = 0.0;
        double n = 0.0;
        for(int i = 0; i < cm.length; i++)
        {
            sumProd += (rowSums[i] * colSums[i]);
            uniProd += (colSums[i] * colSums[i]);
            n += rowSums[i];
        }
        if(n <= 0)
        {
            throw new MetricCalculationException("The sum product of the rows and columns in the contingency table "
                + "must exceed zero when computing the '" + this + "': " + n);
        }
        //Compose the result
        final double nSquared = n * n;
        final double result = ((diag / n) - (sumProd / nSquared)) / (1.0 - (uniProd / nSquared));
        return getDataFactory().ofScalarOutput(result, getMetadata(output));
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

    protected static class PeirceSkillScoreBuilder<S extends MulticategoryPairs> extends MetricBuilder<S, ScalarOutput>
    {

        @Override
        public PeirceSkillScore<S> build()
        {
            return new PeirceSkillScore<>(this);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private PeirceSkillScore(final PeirceSkillScoreBuilder<S> builder)
    {
        super(builder);
    }

}
