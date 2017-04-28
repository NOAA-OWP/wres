package wres.engine.statistics.metric;

import java.util.Objects;

import wres.engine.statistics.metric.inputs.MetricInputException;
import wres.engine.statistics.metric.inputs.MulticategoryPairs;
import wres.engine.statistics.metric.outputs.MatrixOutput;
import wres.engine.statistics.metric.outputs.MetricOutput;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;
import wres.engine.statistics.metric.outputs.ScalarOutput;
import wres.engine.statistics.metric.parameters.MetricParameter;

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
implements Score, Collectable<S, MetricOutput<?, ?>, T>
{

    @Override
    public T apply(final S s)
    {
        return apply(getCollectionInput(s));
    }

    @Override
    public void checkParameters(final MetricParameter... par)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public String getName()
    {
        return "Peirce Skill Score";
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
    public T apply(final MetricOutput<?, ?> output)
    {
        Objects.requireNonNull(output, "Specify non-null input for the '" + toString() + "'.");
        if(!(output instanceof MatrixOutput))
        {
            throw new MetricInputException("Expected an intermediate result with the Contingency Table when "
                + "computing the '" + this + "'.");
        }
        final MatrixOutput v = (MatrixOutput)output;
        final double[][] cm = v.getData().getValues();

        //Dichotomous predictand
        if(v.getData().size() == 4)
        {
            return MetricOutputFactory.getExtendsScalarOutput((cm[0][0] / (cm[0][0] + cm[1][0]))
                - (cm[0][1] / (cm[0][1] + cm[1][1])), v.getSampleSize().valueOf(), output.getDimension());
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
        //Compose the result
        final double nSquared = n * n;
        final double result = ((diag / n) - (sumProd / nSquared)) / (1.0 - (uniProd / nSquared));
        return MetricOutputFactory.getExtendsScalarOutput(result, v.getSampleSize().valueOf(), output.getDimension());
    }

    @Override
    public MetricOutput<?, ?> getCollectionInput(final S input)
    {
        return super.apply(input); //Contingency table
    }

    @Override
    public String getCollectionOf()
    {
        return super.getName();
    }

    /**
     * Protected constructor.
     */

    protected PeirceSkillScore()
    {
        super();
    }

}
