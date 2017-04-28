package wres.engine.statistics.metric;

import java.util.Objects;

import wres.engine.statistics.metric.inputs.DichotomousPairs;
import wres.engine.statistics.metric.outputs.MatrixOutput;
import wres.engine.statistics.metric.outputs.MetricOutput;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;
import wres.engine.statistics.metric.outputs.ScalarOutput;
import wres.engine.statistics.metric.parameters.MetricParameter;

/**
 * The Equitable Threat Score (ETS) is a dichotomous measure of the fraction of all predicted outcomes that occurred
 * (i.e. were true positives), after factoring out the correct predictions that were due to chance.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class EquitableThreatScore<S extends DichotomousPairs, T extends ScalarOutput>
extends
    ContingencyTable<S, T>
implements Score, Collectable<S, MetricOutput<?, ?>, T>
{

    /**
     * Return a default {@link EquitableThreatScore} function.
     * 
     * @return a default {@link EquitableThreatScore} function.
     */

    public static EquitableThreatScore<DichotomousPairs, ScalarOutput> newInstance()
    {
        return new EquitableThreatScore<DichotomousPairs, ScalarOutput>();
    }

    @Override
    public T apply(final S s)
    {
        Objects.requireNonNull(s, "Specify non-null input for the '" + toString() + "'.");
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
        return "Equitable Threat Score";
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
        is2x2ContingencyTable(output, this);
        final MatrixOutput v = (MatrixOutput)output;
        final double[][] cm = v.getData().getValues();
        final double t = cm[0][0] + cm[0][1] + cm[1][0];
        final double hitsRandom = ((cm[0][0] + cm[1][0]) * (cm[0][0] + cm[0][1])) / (t + cm[1][1]);
        return MetricOutputFactory.getExtendsScalarOutput((cm[0][0] - hitsRandom) / (t - hitsRandom),
                                                          v.getSampleSize().valueOf(),
                                                          null);
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
     * Prevent direct construction.
     */

    protected EquitableThreatScore()
    {
        super();
    }

}
