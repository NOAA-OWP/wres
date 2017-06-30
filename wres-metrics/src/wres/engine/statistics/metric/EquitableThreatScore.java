package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.metric.DichotomousPairs;
import wres.datamodel.metric.MatrixOutput;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutput;
import wres.datamodel.metric.MetricOutputFactory;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.ScalarOutput;

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
implements Score, Collectable<S, MetricOutput<?>, T>
{

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class EquitableThreatScoreBuilder<S extends DichotomousPairs, T extends ScalarOutput>
    implements MetricBuilder<S, T>
    {

        @Override
        public EquitableThreatScore<S, T> build()
        {
            return new EquitableThreatScore<>();
        }

    }

    @Override
    public T apply(final S s)
    {
        Objects.requireNonNull(s, "Specify non-null input for the '" + toString() + "'.");
        return apply(getCollectionInput(s));
    }

    @Override
    public T apply(final MetricOutput<?> output)
    {
        is2x2ContingencyTable(output, this);
        final MatrixOutput v = (MatrixOutput)output;
        final double[][] cm = v.getData().getDoubles();
        final double t = cm[0][0] + cm[0][1] + cm[1][0];
        final double hitsRandom = ((cm[0][0] + cm[1][0]) * (cm[0][0] + cm[0][1])) / (t + cm[1][1]);
        //Metadata
        final MetricOutputMetadata metIn = output.getMetadata();
        final MetricOutputMetadata metOut = MetadataFactory.getMetadata(metIn.getSampleSize(),
                                                                        metIn.getDimension(),
                                                                        getID(),
                                                                        MetricConstants.MAIN,
                                                                        metIn.getID(),
                                                                        null);
        return MetricOutputFactory.ofExtendsScalarOutput((cm[0][0] - hitsRandom) / (t - hitsRandom), metOut);
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.EQUITABLE_THREAT_SCORE;
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
    public MetricConstants getDecompositionID()
    {
        return MetricConstants.NONE;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }        
    
    @Override
    public MetricOutput<?> getCollectionInput(final S input)
    {
        return super.apply(input); //2x2 contingency table
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return super.getID();
    }

    /**
     * Hidden constructor.
     */

    private EquitableThreatScore()
    {
        super();
    }

}
