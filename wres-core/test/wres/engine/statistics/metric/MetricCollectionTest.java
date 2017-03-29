package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.engine.statistics.metric.inputs.DichotomousPairs;
import wres.engine.statistics.metric.inputs.DiscreteProbabilityPairs;
import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.outputs.MetricOutput;
import wres.engine.statistics.metric.outputs.MetricOutputCollection;
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
        n.add(MeanError.newInstance()); //Should be -200.55
        n.add(MeanAbsoluteError.newInstance()); //Should be 201.37
        n.add(RootMeanSquareError.newInstance()); //Should be 632.4586381732801

        //Compute them
        final MetricOutputCollection<ScalarOutput> d = n.apply(input);

        //Print them
        //d.stream().forEach(g -> System.out.println(g.valueOf()));

        //Check them
        assertTrue(d.get(0).equals(new ScalarOutput(-200.55, 10)));
        assertTrue(d.get(1).equals(new ScalarOutput(201.37, 10)));
        assertTrue(d.get(2).equals(new ScalarOutput(632.4586381732801, 10)));
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
        m.add(CriticalSuccessIndex.newInstance()); //Should be 0.5734265734265734
        m.add(ProbabilityOfDetection.newInstance()); //Should be 0.780952380952381
        m.add(ProbabilityOfFalseDetection.newInstance()); //Should be 0.14615384615384616
        m.add(PeirceSkillScore.newInstance()); //Should be 0.6347985347985348
        m.add(EquitableThreatScore.newInstance()); //Should be 0.43768152544513195

        //Compute them
        final MetricOutputCollection<ScalarOutput> c = m.apply(input);

        //Print them
        //c.stream().forEach(g -> System.out.println(g.valueOf()));

        //Check them
        assertTrue(c.get(0).equals(new ScalarOutput(0.5734265734265734, 365)));
        assertTrue(c.get(1).equals(new ScalarOutput(0.780952380952381, 365)));
        assertTrue(c.get(2).equals(new ScalarOutput(0.14615384615384616, 365)));
        assertTrue(c.get(3).equals(new ScalarOutput(0.6347985347985348, 365)));
        assertTrue(c.get(4).equals(new ScalarOutput(0.43768152544513195, 365)));
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
        final MetricCollection<DiscreteProbabilityPairs, MetricOutput> n =
                                                                         MetricCollection.ofDiscreteProbabilityOutput();
        //Add some appropriate metrics to the collection
        n.add(BrierScore.newInstance()); //Should be 0.26
        n.add(BrierSkillScore.newInstance()); //Should be 0.0

        //Compute them
        final MetricOutputCollection<MetricOutput> d = n.apply(input);

        //Print them
        //d.stream().forEach(g -> System.out.println(((ScalarOutput)g).valueOf()));

        //Check them
        assertTrue(d.get(0).equals(new ScalarOutput(0.26, 6)));
        assertTrue(d.get(1).equals(new ScalarOutput(0.0, 6)));
    }

}
