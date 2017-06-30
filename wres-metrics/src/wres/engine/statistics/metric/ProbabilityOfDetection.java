package wres.engine.statistics.metric;

import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricOutput;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.engine.statistics.metric.inputs.DichotomousPairs;
import wres.engine.statistics.metric.outputs.MatrixOutput;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;
import wres.engine.statistics.metric.outputs.ScalarOutput;

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
implements Score, Collectable<S, MetricOutput<?>, T>
{

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class ProbabilityOfDetectionBuilder<S extends DichotomousPairs, T extends ScalarOutput>
    implements MetricBuilder<S, T>
    {

        @Override
        public ProbabilityOfDetection<S, T> build()
        {
            return new ProbabilityOfDetection<>();
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
        is2x2ContingencyTable(output, this);
        final MatrixOutput v = (MatrixOutput)output;
        final double[][] cm = v.getData().getDoubles();
        //Metadata
        final MetricOutputMetadata metIn = output.getMetadata();
        final MetricOutputMetadata metOut =
                                  MetadataFactory.getMetadata(metIn.getSampleSize(),
                                                              metIn.getDimension(),
                                                              getID(),
                                                              MetricConstants.MAIN,
                                                              metIn.getID(),
                                                              null); 
        return MetricOutputFactory.ofExtendsScalarOutput(cm[0][0] / (cm[0][0] + cm[1][0]), metOut);
    }

    @Override
    public int getID()
    {
        return MetricConstants.PROBABILITY_OF_DETECTION;
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
        return super.apply(input); //2x2 contingency table
    }

    @Override
    public int getCollectionOf()
    {
        return super.getID();
    }

    /**
     * Hidden constructor.
     */

    private ProbabilityOfDetection()
    {
        super();
    }

}
