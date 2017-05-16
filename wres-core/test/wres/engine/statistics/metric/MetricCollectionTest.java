package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.engine.statistics.metric.inputs.DichotomousPairs;
import wres.engine.statistics.metric.inputs.DiscreteProbabilityPairs;
import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.outputs.MetricOutputCollection;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;
import wres.engine.statistics.metric.outputs.ScalarOutput;

/**
 * Test class for {@link MetricCollection}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class MetricCollectionTest
{

    /**
     * Construct a collection of metrics that consume single-valued pairs and produce scalar outputs. Compute and check
     * the results.
     */

    @Test
    public void test1MetricCollection()
    {
        //Generate some data
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Create a collection of metrics that consume single-valued pairs and produce a scalar output
        final MetricCollection<SingleValuedPairs, ScalarOutput> n = MetricCollection.ofSingleValuedScalar();

        //Add some appropriate metrics to the collection
        n.add(MetricFactory.ofMeanError()); //Should be -200.55
        n.add(MetricFactory.ofMeanAbsoluteError()); //Should be 201.37
        n.add(MetricFactory.ofRootMeanSquareError()); //Should be 632.4586381732801

        //Compute them
        final MetricOutputCollection<ScalarOutput> d = n.apply(input);

        //Print them
        d.stream().forEach(g -> System.out.println(g.getData()));

        //Check them
        assertTrue(d.get(0).equals(MetricOutputFactory.getScalarOutput(-200.55, 10, null)));
        assertTrue(d.get(1).equals(MetricOutputFactory.getScalarOutput(201.37, 10, null)));
        assertTrue(d.get(2).equals(MetricOutputFactory.getScalarOutput(632.4586381732801, 10, null)));
    }

    /**
     * Construct a collection of metrics that consume dichotomous pairs and produce scalar outputs. Compute and check
     * the results.
     */

    @Test
    public void test2MetricCollection()
    {
        //Generate some data
        final DichotomousPairs input = MetricTestDataFactory.getDichotomousPairsOne();

        //Create a collection of dichotomous metrics that produce a scalar output. Since all scores implement 
        //Collectable, they make efficient use of common intermediate data. In this case, all scores require the 2x2
        //Contingency Table, which is computed only once
        final MetricCollection<DichotomousPairs, ScalarOutput> m = MetricCollection.ofDichotomousScalar();

        //Add some appropriate metrics to the collection
        m.add(MetricFactory.ofCriticalSuccessIndex()); //Should be 0.5734265734265734
        m.add(MetricFactory.ofProbabilityOfDetection()); //Should be 0.780952380952381
        m.add(MetricFactory.ofProbabilityOfFalseDetection()); //Should be 0.14615384615384616
        m.add(MetricFactory.ofPeirceSkillScore()); //Should be 0.6347985347985348
        m.add(MetricFactory.ofEquitableThreatScore()); //Should be 0.43768152544513195

        //Compute them
        final MetricOutputCollection<ScalarOutput> c = m.apply(input);

        //Print them
        //c.stream().forEach(g -> System.out.println(g.getData().valueOf()));

        //Check them
        assertTrue(c.get(0).equals(MetricOutputFactory.getScalarOutput(0.5734265734265734, 365, null)));
        assertTrue(c.get(1).equals(MetricOutputFactory.getScalarOutput(0.780952380952381, 365, null)));
        assertTrue(c.get(2).equals(MetricOutputFactory.getScalarOutput(0.14615384615384616, 365, null)));
        assertTrue(c.get(3).equals(MetricOutputFactory.getScalarOutput(0.6347985347985348, 365, null)));
        assertTrue(c.get(4).equals(MetricOutputFactory.getScalarOutput(0.43768152544513195, 365, null)));
    }

    /**
     * Construct a collection of metrics that consume discrete probability pairs and produce varying types of outputs,
     * depending on the metric parameters. Compute and check the results.
     */

    @Test
    public void test3MetricCollection()
    {
        //Generate some data
        final DiscreteProbabilityPairs input = MetricTestDataFactory.getDiscreteProbabilityPairsOne();

        //Create a collection metrics that consume probabilistic pairs and produce varying outputs (in principle)
        final MetricCollection<DiscreteProbabilityPairs, ScalarOutput> n =
                                                                         MetricCollection.ofDiscreteProbabilityScalarOutput();
        //Add some appropriate metrics to the collection
        n.add(MetricFactory.ofBrierScoreNoDecomp()); //Should be 0.26
        n.add(MetricFactory.ofBrierSkillScoreNoDecomp()); //Should be 0.0

        //Compute them
        final MetricOutputCollection<ScalarOutput> d = n.apply(input);

        //Print them
        //d.stream().forEach(g -> System.out.println(((ScalarOutput)g).getData().valueOf()));

        //Check them
        assertTrue(d.get(0).equals(MetricOutputFactory.getScalarOutput(0.26, 6, null)));
        assertTrue(d.get(1).equals(MetricOutputFactory.getScalarOutput(0.0, 6, null)));
    }

}
