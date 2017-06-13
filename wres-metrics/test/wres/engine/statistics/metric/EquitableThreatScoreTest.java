package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.engine.statistics.metric.EquitableThreatScore.EquitableThreatScoreBuilder;
import wres.engine.statistics.metric.inputs.DichotomousPairs;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;
import wres.engine.statistics.metric.outputs.ScalarOutput;

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
        //Generate some data
        final DichotomousPairs input = MetricTestDataFactory.getDichotomousPairsOne();

        //Build the metric
        final EquitableThreatScoreBuilder<DichotomousPairs, ScalarOutput> b =
                                                                            new EquitableThreatScore.EquitableThreatScoreBuilder<>();
        final EquitableThreatScore<DichotomousPairs, ScalarOutput> ets = b.build();

        //Check the results
        assertTrue(ets.apply(input).equals(MetricOutputFactory.ofScalarOutput(0.43768152544513195, 365, null)));
        //Check the parameters
        assertTrue("Unexpected name for the Equitable Threat Score.", ets.getName().equals("Equitable Threat Score"));
        assertTrue("The Equitable Threat Score is not decomposable.", !ets.isDecomposable());
        assertTrue("The Equitable Threat Score is a skill score.", ets.isSkillScore());
        assertTrue("The Equitable Threat Score cannot be decomposed.",
                   ets.getDecompositionID() == MetricConstants.NONE);
        final String name = MetricFactory.ofContingencyTable().getName();
        assertTrue("The Equitable Threat Score should be a collection of '" + name
            + "', but is actually a collection of '" + ets.getCollectionOf() + "'.",
                   ets.getCollectionOf().equals(name));

    }

}
