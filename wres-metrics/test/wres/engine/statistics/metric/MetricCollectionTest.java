package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import java.util.function.BiPredicate;

import org.junit.Test;

import wres.engine.statistics.metric.inputs.DichotomousPairs;
import wres.engine.statistics.metric.inputs.DiscreteProbabilityPairs;
import wres.engine.statistics.metric.inputs.MulticategoryPairs;
import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.outputs.MetricOutputCollection;
import wres.engine.statistics.metric.outputs.ScalarOutput;
import wres.engine.statistics.metric.outputs.VectorOutput;

/**
 * Tests the {@link MetricCollection}.
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
    public void test1OfSingleValuedScalar()
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
        //d.stream().forEach(g -> System.out.println(g.getData()));

        //Check them   
        final Double expectedFirst = -200.55;
        final Double expectedSecond = 201.37;
        final Double expectedThird = 632.4586381732801;
        final Double actualFirst = d.get(0).getData();
        final Double actualSecond = d.get(1).getData();
        final Double actualThird = d.get(2).getData();

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue("Expected value: " + expectedFirst + ". Actual value: " + actualFirst + ".",
                   testMe.test(actualFirst, expectedFirst));
        assertTrue("Expected value: " + expectedSecond + ". Actual value: " + actualSecond + ".",
                   testMe.test(actualSecond, expectedSecond));
        assertTrue("Expected value: " + expectedThird + ". Actual value: " + actualThird + ".",
                   testMe.test(actualThird, expectedThird));
    }

    /**
     * Construct a collection of metrics that consume dichotomous pairs and produce scalar outputs. Compute and check
     * the results.
     */

    @Test
    public void test2OfDichotomousScalar()
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
        //c.stream().forEach(g -> System.out.println(g.getData().doubleValue()));

        //Check them
        final Double expectedFirst = 0.5734265734265734;
        final Double expectedSecond = 0.780952380952381;
        final Double expectedThird = 0.14615384615384616;
        final Double expectedFourth = 0.6347985347985348;
        final Double expectedFifth = 0.43768152544513195;
        final Double actualFirst = c.get(0).getData();
        final Double actualSecond = c.get(1).getData();
        final Double actualThird = c.get(2).getData();
        final Double actualFourth = c.get(3).getData();
        final Double actualFifth = c.get(4).getData();

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue("Expected value: " + expectedFirst + ". Actual value: " + actualFirst + ".",
                   testMe.test(actualFirst, expectedFirst));
        assertTrue("Expected value: " + expectedSecond + ". Actual value: " + actualSecond + ".",
                   testMe.test(actualSecond, expectedSecond));
        assertTrue("Expected value: " + expectedThird + ". Actual value: " + actualThird + ".",
                   testMe.test(actualThird, expectedThird));
        assertTrue("Expected value: " + expectedFourth + ". Actual value: " + actualFourth + ".",
                   testMe.test(actualFourth, expectedFourth));
        assertTrue("Expected value: " + expectedFifth + ". Actual value: " + actualFifth + ".",
                   testMe.test(actualFifth, expectedFifth));
    }

    /**
     * Construct a collection of metrics that consume discrete probability pairs and produce vector outputs. Compute and
     * check the results.
     */

    @Test
    public void test3OfDiscreteProbabilityVector()
    {
        //Generate some data
        final DiscreteProbabilityPairs input = MetricTestDataFactory.getDiscreteProbabilityPairsTwo();

        //Create a collection metrics that consume probabilistic pairs and generate vector outputs
        final MetricCollection<DiscreteProbabilityPairs, VectorOutput> n =
                                                                         MetricCollection.ofDiscreteProbabilityVectorOutput();
        //Add some appropriate metrics to the collection
        n.add(MetricFactory.ofBrierScore()); //Should be 0.26
        n.add(MetricFactory.ofBrierSkillScore()); //Should be 0.11363636363636376

        //Compute them
        final MetricOutputCollection<VectorOutput> d = n.apply(input);

        //Print them
        //d.stream().forEach(g -> System.out.println(((ScalarOutput)g).getData().valueOf()));

        //Check them
        final Double expectedFirst = 0.26;
        final Double expectedSecond = 0.11363636363636376;
        final Double actualFirst = d.get(0).getData().getDoubles()[0];
        final Double actualSecond = d.get(1).getData().getDoubles()[0];

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue("Expected value: " + expectedFirst + ". Actual value: " + actualFirst + ".",
                   testMe.test(expectedFirst, actualFirst));
        assertTrue("Expected value: " + expectedSecond + ". Actual value: " + actualSecond + ".",
                   testMe.test(expectedFirst, actualFirst));
    }

    /**
     * Construct a collection of metrics that consume single-valued pairs and produce vector outputs. Compute and check
     * the results.
     */

    @Test
    public void test4OfSingleValuedVector()
    {
        //Generate some data
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsTwo();

        //Create a collection metrics that consume single-valued pairs and produce vector outputs
        final MetricCollection<SingleValuedPairs, VectorOutput> n = MetricCollection.ofSingleValuedVector();
        //Add some appropriate metrics to the collection
        n.add(MetricFactory.ofMeanSquareError()); //Should be 400003.929
        n.add(MetricFactory.ofMeanSquareErrorSkillScore()); //Should be 0.8007025335093799

        //Compute them
        final MetricOutputCollection<VectorOutput> d = n.apply(input);

        //Print them
        //d.stream().forEach(g -> System.out.println(((ScalarOutput)g).getData().valueOf()));

        //Check them
        final Double expectedFirst = 400003.929;
        final Double expectedSecond = 0.8007025335093799;
        final Double actualFirst = d.get(0).getData().getDoubles()[0];
        final Double actualSecond = d.get(1).getData().getDoubles()[0];

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue("Expected value: " + expectedFirst + ". Actual value: " + actualFirst + ".",
                   testMe.test(expectedFirst, actualFirst));
        assertTrue("Expected value: " + expectedSecond + ". Actual value: " + actualSecond + ".",
                   testMe.test(expectedFirst, actualFirst));
    }

    /**
     * Construct a collection of metrics that consume multicategory pairs and produce scalar outputs. Compute and check
     * the results.
     */

    @Test
    public void test5OfMulticategoryScalar()
    {
        //Generate some data
        final MulticategoryPairs input = MetricTestDataFactory.getMulticategoryPairsOne();

        //Create a collection of multicategory metrics that produce a scalar output. 
        final MetricCollection<MulticategoryPairs, ScalarOutput> m = MetricCollection.ofMulticategoryScalar();

        //Add some appropriate metrics to the collection
        m.add(MetricFactory.ofPeirceSkillScoreMulti()); //Should be 0.05057466520850963

        //Compute them
        final MetricOutputCollection<ScalarOutput> c = m.apply(input);

        //Print them
        //c.stream().forEach(g -> System.out.println(g.getData().doubleValue()));

        //Check them
        final Double expectedFirst = 0.05057466520850963;
        final Double actualFirst = c.get(0).getData();

        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();

        assertTrue("Expected value: " + expectedFirst + ". Actual value: " + actualFirst + ".",
                   testMe.test(actualFirst, expectedFirst));
    }

}
