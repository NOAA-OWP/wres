package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.metric.DefaultMetricOutputFactory;
import wres.datamodel.metric.DichotomousPairs;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputFactory;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.engine.statistics.metric.EquitableThreatScore.EquitableThreatScoreBuilder;

/**
 * Tests the {@link EquitableThreatScore}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class EquitableThreatScoreTest
{

    /**
     * Constructs a {@link EquitableThreatScore} and compares the actual result to the expected result. Also, checks the
     * parameters of the metric.
     */

    @Test
    public void test1EquitableThreatScore()
    {
        //Obtain the factories
        final MetricOutputFactory outF = DefaultMetricOutputFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();

        //Generate some data
        final DichotomousPairs input = MetricTestDataFactory.getDichotomousPairsOne();

        //Metadata for the output
        final MetricOutputMetadata m1 = metaFac.getMetadata(input.getData().size(),
                                                            metaFac.getDimension(),
                                                            MetricConstants.EQUITABLE_THREAT_SCORE,
                                                            MetricConstants.MAIN,
                                                            "Main",
                                                            null);

        //Build the metric
        final EquitableThreatScoreBuilder b = new EquitableThreatScore.EquitableThreatScoreBuilder();
        b.setOutputFactory(outF);
        final EquitableThreatScore ets = b.build();

        //Check the results
        final MetricFactory metF = MetricFactory.getInstance(outF);
        assertTrue(ets.apply(input).equals(outF.ofScalarOutput(0.43768152544513195, m1)));
        //Check the parameters
        assertTrue("Unexpected name for the Equitable Threat Score.",
                   ets.getName().equals(metaFac.getMetricName(MetricConstants.EQUITABLE_THREAT_SCORE)));
        assertTrue("The Equitable Threat Score is not decomposable.", !ets.isDecomposable());
        assertTrue("The Equitable Threat Score is a skill score.", ets.isSkillScore());
        assertTrue("The Equitable Threat Score cannot be decomposed.",
                   ets.getDecompositionID() == MetricConstants.NONE);
        final String expName = metF.ofContingencyTable().getName();
        final String actName = metaFac.getMetricName(ets.getCollectionOf());
        assertTrue("The Equitable Threat Score should be a collection of '" + expName
            + "', but is actually a collection of '" + actName + "'.",
                   ets.getCollectionOf() == metF.ofContingencyTable().getID());

    }

}
