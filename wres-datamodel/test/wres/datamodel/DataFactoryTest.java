package wres.datamodel;

import static org.junit.Assert.*;

import org.junit.Test;

public class DataFactoryTest
{
    private static final wres.datamodel.DataFactory dataFactory =
        wres.datamodel.DataFactory.instance();

    public static final double THRESHOLD = 0.00001;

    @Test
    public void pairOfTest()
    {
        //Reference the constant member for a concrete instance of the factory
        final PairOfDoubles tuple = dataFactory.pairOf(1.0, 2.0);
        assertNotNull(tuple);
        assertEquals(tuple.getItemOne(), 1.0, THRESHOLD);
        assertEquals(tuple.getItemTwo(), 2.0, THRESHOLD);
    }

    @Test
    public void vectorOfDoublesTest()
    {
        double[] arrOne = {1.0, 2.0};
        final VectorOfDoubles doubleVecOne = dataFactory.vectorOf(arrOne);
        assertNotNull(doubleVecOne);
        assertEquals(doubleVecOne.getDoubles()[0], 1.0, THRESHOLD);
        assertEquals(doubleVecOne.getDoubles()[1], 2.0, THRESHOLD);
    }

    @Test
    public void vectorOfDoublesMutationTest()
    {
        double[] arrOne = {1.0, 2.0};
        final VectorOfDoubles doubleVecOne = dataFactory.vectorOf(arrOne);
        arrOne[0] = 3.0;
        arrOne[1] = 4.0;
        assertNotNull(doubleVecOne);
        assertEquals(doubleVecOne.getDoubles()[0], 1.0, THRESHOLD);
        assertEquals(doubleVecOne.getDoubles()[1], 2.0, THRESHOLD);
    }

    @Test
    public void pairOfVectorsTest()
    {
        double[] arrOne = {1.0, 2.0, 3.0};
        double[] arrTwo = {4.0, 5.0};
        final Pair<VectorOfDoubles,VectorOfDoubles> pair = dataFactory.pairOf(arrOne, arrTwo);
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
        double[] arrOne = {2.0, 3.0};
        final PairOfDoubleAndVectorOfDoubles tuple = dataFactory.pairOf(1.0, arrOne);
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
        double[] arrOne = {2.0, 3.0};
        final PairOfDoubleAndVectorOfDoubles tuple = dataFactory.pairOf(1.0, arrOne);
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
        Double[] arrOne = {2.0, 3.0};
        final PairOfDoubleAndVectorOfDoubles tuple = dataFactory.pairOf(1.0, arrOne);
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
        boolean[] arrOne = {false, true};
        final VectorOfBooleans vec = dataFactory.vectorOf(arrOne);
        assertEquals(vec.getBooleans()[0], false);
        assertEquals(vec.getBooleans()[1], true);
    }

    @Test
    public void vectorOfBooleanMutationTest()
    {
        boolean[] arrOne = {false, true};
        final VectorOfBooleans vec = dataFactory.vectorOf(arrOne);
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
        boolean one = true;
        boolean two = false;
        final PairOfBooleans bools = dataFactory.pairOf(one, two);
        assertEquals(true, bools.getItemOne());
        assertEquals(false, bools.getItemTwo());
    }

    @Test
    public void pairOfBooleansMutationTest()
    {
        boolean one = true;
        boolean two = false;
        final PairOfBooleans bools = dataFactory.pairOf(one, two);
        one = false;
        two = true;
        assertEquals(true, bools.getItemOne());
        assertEquals(false, bools.getItemTwo());
    }
}
