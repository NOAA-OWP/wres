package wres.datamodel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.VectorOfBooleans;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.pairs.Pair;
import wres.datamodel.inputs.pairs.PairOfBooleans;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;

/**
 * Tests the {@link DefaultDataFactory}.
 * 
 * @author james.brown@hydrosolved.com
 * @jesse
 * @version 0.1
 * @since 0.1
 */
public final class DefaultDataFactoryTest
{

    public static final double THRESHOLD = 0.00001;

    private final DataFactory metIn = DefaultDataFactory.getInstance();

    /**
     * Tests the pairing methods in {@link DefaultDataFactory}.
     */

    @Test
    public void test1MetricFactory()
    {
        final MetadataFactory metaFac = DefaultMetadataFactory.getInstance();
        final Metadata m1 = metaFac.getMetadata(metaFac.getDimension(),
                                                metaFac.getDatasetIdentifier("DRRC2", "SQIN", "HEFS"));
        final List<VectorOfBooleans> input = new ArrayList<>();
        input.add(metIn.vectorOf(new boolean[]{true, false}));
        metIn.ofDichotomousPairs(input, m1);
        metIn.ofMulticategoryPairs(input, m1);

        final List<PairOfDoubles> dInput = new ArrayList<>();
        dInput.add(metIn.pairOf(0.0, 1.0));
        final Metadata m2 = metaFac.getMetadata(metaFac.getDimension(),
                                                metaFac.getDatasetIdentifier("DRRC2", "SQIN", "HEFS"));
        final Metadata m3 = metaFac.getMetadata(metaFac.getDimension(),
                                                metaFac.getDatasetIdentifier("DRRC2", "SQIN", "ESP"));
        metIn.ofDiscreteProbabilityPairs(dInput, m2);
        metIn.ofDiscreteProbabilityPairs(dInput, dInput, m2, m3, null);
        metIn.ofSingleValuedPairs(dInput, m3);
        metIn.ofSingleValuedPairs(dInput, dInput, m2, m3, null);
        
        final List<PairOfDoubleAndVectorOfDoubles> eInput = new ArrayList<>();
        eInput.add(metIn.pairOf(0.0, new double[]{1.0,2.0}));
        metIn.ofEnsemblePairs(eInput, m3);
        metIn.ofEnsemblePairs(eInput, eInput, m2, m3, null);       
    }

    @Test
    public void pairOfTest()
    {
        //Reference the constant member for a concrete instance of the factory
        final PairOfDoubles tuple = metIn.pairOf(1.0, 2.0);
        assertNotNull(tuple);
        assertEquals(tuple.getItemOne(), 1.0, THRESHOLD);
        assertEquals(tuple.getItemTwo(), 2.0, THRESHOLD);
    }

    @Test
    public void vectorOfDoublesTest()
    {
        final double[] arrOne = {1.0, 2.0};
        final VectorOfDoubles doubleVecOne = metIn.vectorOf(arrOne);
        assertNotNull(doubleVecOne);
        assertEquals(doubleVecOne.getDoubles()[0], 1.0, THRESHOLD);
        assertEquals(doubleVecOne.getDoubles()[1], 2.0, THRESHOLD);
    }

    @Test
    public void vectorOfDoublesMutationTest()
    {
        final double[] arrOne = {1.0, 2.0};
        final VectorOfDoubles doubleVecOne = metIn.vectorOf(arrOne);
        arrOne[0] = 3.0;
        arrOne[1] = 4.0;
        assertNotNull(doubleVecOne);
        assertEquals(doubleVecOne.getDoubles()[0], 1.0, THRESHOLD);
        assertEquals(doubleVecOne.getDoubles()[1], 2.0, THRESHOLD);
    }

    @Test
    public void pairOfVectorsTest()
    {
        final double[] arrOne = {1.0, 2.0, 3.0};
        final double[] arrTwo = {4.0, 5.0};
        final Pair<VectorOfDoubles, VectorOfDoubles> pair = metIn.pairOf(arrOne, arrTwo);
        assertNotNull(pair);
        assertEquals(pair.getItemOne().getDoubles()[0], 1.0, THRESHOLD);
        assertEquals(pair.getItemOne().getDoubles()[1], 2.0, THRESHOLD);
        assertEquals(pair.getItemOne().getDoubles()[2], 3.0, THRESHOLD);
        assertEquals(pair.getItemTwo().getDoubles()[0], 4.0, THRESHOLD);
        assertEquals(pair.getItemTwo().getDoubles()[1], 5.0, THRESHOLD);
    }

    @Test
    public void pairOfDoubleAndVectorOfDoublesTest()
    {
        final double[] arrOne = {2.0, 3.0};
        final PairOfDoubleAndVectorOfDoubles tuple = metIn.pairOf(1.0, arrOne);
        assertNotNull(tuple);
        assertEquals(tuple.getItemOne(), 1.0, THRESHOLD);
        assertEquals(tuple.getItemTwo()[0], 2.0, THRESHOLD);
        assertEquals(tuple.getItemTwo()[1], 3.0, THRESHOLD);
        // check that toString() does not throw exception and is not null
        assertNotNull(tuple.toString());
    }

    @Test
    public void pairOfDoubleAndVectorOfDoublesMutationTest()
    {
        final double[] arrOne = {2.0, 3.0};
        final PairOfDoubleAndVectorOfDoubles tuple = metIn.pairOf(1.0, arrOne);
        arrOne[0] = 4.0;
        arrOne[1] = 5.0;
        assertNotNull(tuple);
        assertEquals(tuple.getItemOne(), 1.0, THRESHOLD);
        assertEquals(tuple.getItemTwo()[0], 2.0, THRESHOLD);
        assertEquals(tuple.getItemTwo()[1], 3.0, THRESHOLD);
    }

    @Test
    public void pairOfDoubleAndVectorOfDoublesUsingBoxedMutationTest()
    {
        final Double[] arrOne = {2.0, 3.0};
        final PairOfDoubleAndVectorOfDoubles tuple = metIn.pairOf(1.0, arrOne);
        assertNotNull(tuple);

        // mutate the original array
        arrOne[0] = 4.0;
        arrOne[1] = 5.0;

        assertEquals(tuple.getItemOne(), 1.0, THRESHOLD);
        assertEquals(tuple.getItemTwo()[0], 2.0, THRESHOLD);
        assertEquals(tuple.getItemTwo()[1], 3.0, THRESHOLD);
        // check that toString() does not throw exception and is not null
        assertNotNull(tuple.toString());
    }

    @Test
    public void vectorOfBooleanTest()
    {
        final boolean[] arrOne = {false, true};
        final VectorOfBooleans vec = metIn.vectorOf(arrOne);
        assertEquals(vec.getBooleans()[0], false);
        assertEquals(vec.getBooleans()[1], true);
    }

    @Test
    public void vectorOfBooleanMutationTest()
    {
        final boolean[] arrOne = {false, true};
        final VectorOfBooleans vec = metIn.vectorOf(arrOne);
        // mutate the values in the original array
        arrOne[0] = true;
        arrOne[1] = false;
        // despite mutation, we should get the same result back
        assertEquals(vec.getBooleans()[0], false);
        assertEquals(vec.getBooleans()[1], true);
    }

    @Test
    public void pairOfBooleansTest()
    {
        final boolean one = true;
        final boolean two = false;
        final PairOfBooleans bools = metIn.pairOf(one, two);
        assertEquals(true, bools.getItemOne());
        assertEquals(false, bools.getItemTwo());
    }

    @Test
    public void pairOfBooleansMutationTest()
    {
        boolean one = true;
        boolean two = false;
        final PairOfBooleans bools = metIn.pairOf(one, two);
        one = false;
        two = true;
        assertEquals(true, bools.getItemOne());
        assertEquals(false, bools.getItemTwo());
    }

    @Test
    public void pairOfDoubleAndVectorOfDoubleToStringTest()
    {
        double[] arr = {123456.0, 78910.0, 111213.0};
        PairOfDoubleAndVectorOfDoubles p = metIn.pairOf(141516.0, arr);
        String result = p.toString();
        assertTrue("12345 expected to show up in toString: " + result,
                   result.contains("12345"));
        assertTrue("7891 expected to show up in toString: " + result,
                    result.contains("7891"));
        assertTrue("11121 expected to show up in toString: " + result,
                result.contains("11121"));
        assertTrue("14151 expected to show up in toString: " + result,
                result.contains("14151"));
    }
}
