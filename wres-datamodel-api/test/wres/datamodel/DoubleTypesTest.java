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
        TuplesOfDoubleBricks testFcFc = new TuplesOfDoubleBricks()
        {
            @Override
            public List<Tuple<DoubleArray, DoubleArray>> getTuplesOfDoubleBricks()
            {
                Tuple<DoubleArray,DoubleArray> tupleA
                    = new Tuple<DoubleArray,DoubleArray>()
                {
                    @Override
                    public DoubleArray getItemOne()
                    {
                        return new DoubleArray()
                        {
                            @Override
                            public double[] getDoubles()
                            {
                                return ensembleA1;
                            }
                        };
                    }

                    @Override
                    public DoubleArray getItemTwo()
                    {
                        return new DoubleArray()
                        {
                            @Override
                            public double[] getDoubles()
                            {
                                return ensembleA2;
                            }
                        };
                    }

                };

                Tuple<DoubleArray,DoubleArray> tupleB
                    = new Tuple<DoubleArray,DoubleArray>()
                {
                    @Override
                    public DoubleArray getItemOne()
                    {
                        return new DoubleArray()
                        {
                            @Override
                            public double[] getDoubles()
                            {
                                return ensembleB1;
                            }
                        };
                    }

                    @Override
                    public DoubleArray getItemTwo()
                    {
                        return new DoubleArray()
                        {
                            @Override
                            public double[] getDoubles()
                            {
                                return ensembleB2;
                            }
                        };
                    }
                };

                List<Tuple<DoubleArray,DoubleArray>> fourBricks = new ArrayList<>(2);
                fourBricks.add(tupleA);
                fourBricks.add(tupleB);
                return fourBricks;
            }
        };

        for (Tuple<DoubleArray,DoubleArray> tup : testFcFc.getTuplesOfDoubleBricks())
        {
            assert(tup.getItemOne() instanceof DoubleArray);
            assert(tup.getItemTwo() instanceof DoubleArray);
        }

        assert(testFcFc.getTuplesOfDoubleBricks()
                       .get(0)
                       .getItemOne()
                       .getDoubles()[0] == 1.0);
        assert(testFcFc.getTuplesOfDoubleBricks()
                       .get(0)
                       .getItemOne()
                       .getDoubles()[1] == 2.0);
        assert(testFcFc.getTuplesOfDoubleBricks()
                       .get(0)
                       .getItemTwo()
                       .getDoubles()[0] == 3.0);
        assert(testFcFc.getTuplesOfDoubleBricks()
                       .get(0)
                       .getItemTwo()
                       .getDoubles()[1] == 4.0);
        assert(testFcFc.getTuplesOfDoubleBricks()
                       .get(1)
                       .getItemOne()
                       .getDoubles()[0] == 5.0);
        assert(testFcFc.getTuplesOfDoubleBricks()
                       .get(1)
                       .getItemOne()
                       .getDoubles()[1] == 6.0);
        assert(testFcFc.getTuplesOfDoubleBricks()
                       .get(1)
                       .getItemTwo()
                       .getDoubles()[0] == 7.0);
        assert(testFcFc.getTuplesOfDoubleBricks()
                       .get(1)
                       .getItemTwo()
                       .getDoubles()[1] == 8.0);
    }
}
