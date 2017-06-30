package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.MatrixOfDoubles;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricOutput;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.engine.statistics.metric.inputs.DichotomousPairs;
import wres.engine.statistics.metric.inputs.MulticategoryPairs;
import wres.engine.statistics.metric.outputs.MatrixOutput;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;
import wres.engine.statistics.metric.outputs.ScalarOutput;

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
public final class PeirceSkillScore<S extends MulticategoryPairs, T extends ScalarOutput> extends ContingencyTable<S, T>
implements Score, Collectable<S, MetricOutput<?>, T>
{

    /**
     * A {@link MetricBuilder} to build the dichotomous metric.
     */

    public static class PeirceSkillScoreBuilder<S extends DichotomousPairs, T extends ScalarOutput>
    implements MetricBuilder<S, T>
    {

        @Override
        public PeirceSkillScore<S, T> build()
        {
            return new PeirceSkillScore<>();
        }

    }

    /**
     * A {@link MetricBuilder} to build the multi-category metric.
     */

    public static class PeirceSkillScoreMulticategoryBuilder<S extends MulticategoryPairs, T extends ScalarOutput>
    implements MetricBuilder<S, T>
    {

        @Override
        public PeirceSkillScore<S, T> build()
        {
            return new PeirceSkillScore<>();
        }

    }

    @Override
    public T apply(final S s)
    {
        return apply(getCollectionInput(s));
    }

    @Override
    public T apply(final MetricOutput<?> output)
    {
        Objects.requireNonNull(output, "Specify non-null input for the '" + toString() + "'.");
        //Check the input
        isContingencyTable(output, this);

        final MatrixOfDoubles v = ((MatrixOutput)output).getData();
        final double[][] cm = v.getDoubles();

        //Metadata
        final MetricOutputMetadata metIn = output.getMetadata();
        final MetricOutputMetadata metOut =
                                  MetadataFactory.getMetadata(metIn.getSampleSize(),
                                                              metIn.getDimension(),
                                                              getID(),
                                                              MetricConstants.MAIN,
                                                              metIn.getID(),
                                                              null);  

        //Dichotomous predictand
        if(v.rows() == 2)
        {
            return MetricOutputFactory.ofExtendsScalarOutput((cm[0][0] / (cm[0][0] + cm[1][0]))
                - (cm[0][1] / (cm[0][1] + cm[1][1])), metOut);
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
        return MetricOutputFactory.ofExtendsScalarOutput(result, metOut);
    }

    @Override
    public int getID()
    {
        return MetricConstants.PEIRCE_SKILL_SCORE;
    }

    @Override
    public boolean isSkillScore()
    {
        return true;
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }
    
    @Override
    public boolean hasRealUnits()
    {
        return false;
    }        

    @Override
    public int getDecompositionID()
    {
        return MetricConstants.NONE;
    }

    @Override
    public MetricOutput<?> getCollectionInput(final S input)
    {
        return super.apply(input); //Contingency table
    }

    @Override
    public int getCollectionOf()
    {
        return super.getID();
    }

    /**
     * Hidden constructor.
     */

    private PeirceSkillScore()
    {
        super();
    }

}
