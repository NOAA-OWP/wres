package wres.datamodel;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class DataFactoryTest
{
    private static final wres.datamodel.DataFactory dataFactory =
        wres.datamodel.DataFactory.instance();

    @Test
    public void pairOfTest()
    {
        //Reference the constant member for a concrete instance of the factory
        final PairOfDoubles tuple = dataFactory.pairOf(1.0, 2.0);
        assertNotNull(tuple);
        assert (tuple.getItemOne() == 1.0);
        assert (tuple.getItemTwo() == 2.0);
    }

    @Test
    public void vectorOfDoublesTest()
    {
        double[] arrOne = {1.0, 2.0};
        final VectorOfDoubles doubleVecOne = dataFactory.vectorOf(arrOne);
        assertNotNull(doubleVecOne);
        assert(doubleVecOne.getDoubles()[0] == 1.0);
        assert(doubleVecOne.getDoubles()[1] == 2.0);
    }

    @Test
    public void vectorOfDoublesMutationTest()
    {
        double[] arrOne = {1.0, 2.0};
        final VectorOfDoubles doubleVecOne = dataFactory.vectorOf(arrOne);
        arrOne[0] = 3.0;
        arrOne[1] = 4.0;
        assertNotNull(doubleVecOne);
        assert(doubleVecOne.getDoubles()[0] == 1.0);
        assert(doubleVecOne.getDoubles()[1] == 2.0);
    }

    @Test
    public void pairOfVectorsTest()
    {
        double[] arrOne = {1.0, 2.0, 3.0};
        double[] arrTwo = {4.0, 5.0};
        final Pair<VectorOfDoubles,VectorOfDoubles> pair = dataFactory.pairOf(arrOne, arrTwo);
        assertNotNull(pair);
        assert(pair.getItemOne().getDoubles()[0] == 1.0);
        assert(pair.getItemOne().getDoubles()[1] == 2.0);
        assert(pair.getItemOne().getDoubles()[2] == 3.0);
        assert(pair.getItemTwo().getDoubles()[0] == 4.0);
        assert(pair.getItemTwo().getDoubles()[1] == 5.0);
    }

    @Test
    public void pairOfDoubleAndVectorOfDoublesTest()
    {
        double[] arrOne = {2.0, 3.0};
        final PairOfDoubleAndVectorOfDoubles tuple = dataFactory.pairOf(1.0, arrOne);
        assertNotNull(tuple);
        assert (tuple.getItemOne() == 1.0);
        assert (tuple.getItemTwo()[0] == 2.0);
        assert (tuple.getItemTwo()[1] == 3.0);
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
        assert (tuple.getItemOne() == 1.0);
        assert (tuple.getItemTwo()[0] == 2.0);
        assert (tuple.getItemTwo()[1] == 3.0);
    }

    @Test
    public void pairOfDoubleAndVectorOfDoublesUsingBoxedMutationTest()
    {
        Double[] arrOne = {2.0, 3.0};
        final PairOfDoubleAndVectorOfDoubles tuple = dataFactory.pairOf(1.0, arrOne);
        assertNotNull(tuple);
        arrOne[0] = 4.0;
        arrOne[1] = 5.0;
        assert (tuple.getItemOne() == 1.0);
        assert (tuple.getItemTwo()[0] == 2.0);
        assert (tuple.getItemTwo()[1] == 3.0);
        // check that toString() does not throw exception and is not null
        assertNotNull(tuple.toString());
    }
}
