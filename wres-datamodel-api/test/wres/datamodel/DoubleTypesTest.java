package wres.datamodel;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class DoubleTypesTest
{
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
        PairsOfVectorOfDoubles testFcFc = new PairsOfVectorOfDoubles()
        {
            @Override
            public List<Pair<VectorOfDoubles, VectorOfDoubles>> getTuplesofDoubleArrays()
            {
                Pair<VectorOfDoubles,VectorOfDoubles> tupleA
                    = new Pair<VectorOfDoubles,VectorOfDoubles>()
                {
                    @Override
                    public VectorOfDoubles getItemOne()
                    {
                        return new VectorOfDoubles()
                        {
                            @Override
                            public double[] getDoubles()
                            {
                                return ensembleA1;
                            }
                        };
                    }

                    @Override
                    public VectorOfDoubles getItemTwo()
                    {
                        return new VectorOfDoubles()
                        {
                            @Override
                            public double[] getDoubles()
                            {
                                return ensembleA2;
                            }
                        };
                    }

                };

                Pair<VectorOfDoubles,VectorOfDoubles> tupleB
                    = new Pair<VectorOfDoubles,VectorOfDoubles>()
                {
                    @Override
                    public VectorOfDoubles getItemOne()
                    {
                        return new VectorOfDoubles()
                        {
                            @Override
                            public double[] getDoubles()
                            {
                                return ensembleB1;
                            }
                        };
                    }

                    @Override
                    public VectorOfDoubles getItemTwo()
                    {
                        return new VectorOfDoubles()
                        {
                            @Override
                            public double[] getDoubles()
                            {
                                return ensembleB2;
                            }
                        };
                    }
                };

                List<Pair<VectorOfDoubles,VectorOfDoubles>> fourBricks = new ArrayList<>(2);
                fourBricks.add(tupleA);
                fourBricks.add(tupleB);
                return fourBricks;
            }
        };

        for (Pair<VectorOfDoubles,VectorOfDoubles> tup : testFcFc.getTuplesofDoubleArrays())
        {
            assert(tup.getItemOne() instanceof VectorOfDoubles);
            assert(tup.getItemTwo() instanceof VectorOfDoubles);
        }

        assert(testFcFc.getTuplesofDoubleArrays()
                       .get(0)
                       .getItemOne()
                       .getDoubles()[0] == 1.0);
        assert(testFcFc.getTuplesofDoubleArrays()
                       .get(0)
                       .getItemOne()
                       .getDoubles()[1] == 2.0);
        assert(testFcFc.getTuplesofDoubleArrays()
                       .get(0)
                       .getItemTwo()
                       .getDoubles()[0] == 3.0);
        assert(testFcFc.getTuplesofDoubleArrays()
                       .get(0)
                       .getItemTwo()
                       .getDoubles()[1] == 4.0);
        assert(testFcFc.getTuplesofDoubleArrays()
                       .get(1)
                       .getItemOne()
                       .getDoubles()[0] == 5.0);
        assert(testFcFc.getTuplesofDoubleArrays()
                       .get(1)
                       .getItemOne()
                       .getDoubles()[1] == 6.0);
        assert(testFcFc.getTuplesofDoubleArrays()
                       .get(1)
                       .getItemTwo()
                       .getDoubles()[0] == 7.0);
        assert(testFcFc.getTuplesofDoubleArrays()
                       .get(1)
                       .getItemTwo()
                       .getDoubles()[1] == 8.0);
    }
}
