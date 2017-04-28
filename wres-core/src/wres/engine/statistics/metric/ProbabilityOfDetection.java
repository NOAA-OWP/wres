package wres.engine.statistics.metric;

import wres.engine.statistics.metric.inputs.DichotomousPairs;
import wres.engine.statistics.metric.outputs.MatrixOutput;
import wres.engine.statistics.metric.outputs.MetricOutput;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;
import wres.engine.statistics.metric.outputs.ScalarOutput;
import wres.engine.statistics.metric.parameters.MetricParameter;

/**
 * The Probability of Detection (PoD) measures the fraction of observed occurrences that were hits.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class ProbabilityOfDetection<S extends DichotomousPairs, T extends ScalarOutput>
extends
    ContingencyTable<S, T>
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
        return "Probability of Detection";
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    @Override
    public T apply(final MetricOutput<?, ?> output)
    {
        is2x2ContingencyTable(output, this);
        final MatrixOutput v = (MatrixOutput)output;
        final double[][] cm = v.getData().getValues();
        return MetricOutputFactory.getExtendsScalarOutput(cm[0][0] / (cm[0][0] + cm[1][0]),
                                                          v.getSampleSize().valueOf(),
                                                          output.getDimension());
    }

    @Override
    public MetricOutput<?, ?> getCollectionInput(final S input)
    {
        return super.apply(input); //2x2 contingency table
    }

    @Override
    public String getCollectionOf()
    {
        return super.getName();
    }

    /**
     * Protected constructor.
     */

    protected ProbabilityOfDetection()
    {
        super();
    }

}
