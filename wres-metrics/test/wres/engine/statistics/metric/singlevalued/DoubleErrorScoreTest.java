package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.outputs.ScalarOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.singlevalued.DoubleErrorScore;
import wres.engine.statistics.metric.singlevalued.MeanError;
import wres.engine.statistics.metric.singlevalued.MeanError.MeanErrorBuilder;

/**
 * Tests the {@link DoubleErrorScore}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class DoubleErrorScoreTest
{

    /**
     * Checks that the baseline is set correctly.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test1Baseline() throws MetricParameterException
    {
        //Obtain the factories
        final DataFactory outF = DefaultDataFactory.getInstance();

        //Generate some data with a baseline
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsTwo();

        //Build the metric: use a mean error, although this would not ordinarily have a baseline
        final MeanErrorBuilder b = new MeanError.MeanErrorBuilder();
        b.setOutputFactory(outF);
        final MeanError me = b.build();

        //Check the results
        final ScalarOutput actual = me.apply(input);

        //Check the parameters
        assertTrue("Unexpected baseline identifier for the DoubleErrorScore.",
                   actual.getMetadata().getIdentifier().getScenarioIDForBaseline().equals("ESP"));
    }

}
