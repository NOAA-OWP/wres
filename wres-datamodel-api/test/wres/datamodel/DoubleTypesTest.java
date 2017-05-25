package wres.datamodel;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class DoubleTypesTest
{

    private static class VectorOfDoublesSimple implements VectorOfDoubles
    {
        private final double[] doubles;

        public VectorOfDoublesSimple(double[] doubles)
        {
            this.doubles = doubles.clone();
        }

        @Override
        public double[] getDoubles()
        {
            return this.doubles.clone();
        }
    }
    /**
     * Test demonstrating the hierarchy of the types
     * 
     * At a low level, we have DoubleBricks which allow getting double[]
     * At a higher level, we have wrapper types describing contents.
     */
    @Test
    public void testCanWeGetDoubleBricksFromHigherOrderTypes()
    {
        double[] ensembleA1 = { 1.0, 2.0 };
        double[] ensembleA2 = { 3.0, 4.0 };
        double[] ensembleB1 = { 5.0, 6.0 };
        double[] ensembleB2 = { 7.0, 8.0 };
        List<Pair<VectorOfDoubles,VectorOfDoubles>> testFcFc = new ArrayList<>(2);

        Pair<VectorOfDoubles,VectorOfDoubles> tupleA = new Pair<VectorOfDoubles,VectorOfDoubles>()
        {
            @Override
            public VectorOfDoubles getItemOne()
            {
                return new VectorOfDoublesSimple(ensembleA1);
            }

            @Override
            public VectorOfDoubles getItemTwo()
            {
                return new VectorOfDoublesSimple(ensembleA2);
            }

        };

        Pair<VectorOfDoubles,VectorOfDoubles> tupleB
            = new Pair<VectorOfDoubles,VectorOfDoubles>()
        {
            @Override
            public VectorOfDoubles getItemOne()
            {
                return new VectorOfDoublesSimple(ensembleB1);
            }

            @Override
            public VectorOfDoubles getItemTwo()
            {
                return new VectorOfDoublesSimple(ensembleB2);
            }
        };

        testFcFc.add(tupleA);
        testFcFc.add(tupleB);

        for (Pair<VectorOfDoubles,VectorOfDoubles> tup : testFcFc)
        {
            assert(tup.getItemOne() instanceof VectorOfDoubles);
            assert(tup.getItemTwo() instanceof VectorOfDoubles);
        }

        assert(testFcFc.get(0)
               .getItemOne()
               .getDoubles()[0] == 1.0);
        assert(testFcFc.get(0)
               .getItemOne()
               .getDoubles()[1] == 2.0);
        assert(testFcFc.get(0)
               .getItemTwo()
               .getDoubles()[0] == 3.0);
        assert(testFcFc.get(0)
               .getItemTwo()
               .getDoubles()[1] == 4.0);
        assert(testFcFc.get(1)
               .getItemOne()
               .getDoubles()[0] == 5.0);
        assert(testFcFc.get(1)
               .getItemOne()
               .getDoubles()[1] == 6.0);
        assert(testFcFc.get(1)
               .getItemTwo()
               .getDoubles()[0] == 7.0);
        assert(testFcFc.get(1)
               .getItemTwo()
               .getDoubles()[1] == 8.0);
    }
}
