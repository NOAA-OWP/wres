package wres.datamodel.impl;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import wres.datamodel.PairOfDoubles;

public class DataFactoryImplTest
{
    @Test
    public void pairOfTest()
    {
        //Reference the constant member for a concrete instance of the factory
        wres.datamodel.DataFactory df = wres.datamodel.impl.DataFactory.instance();
        final PairOfDoubles tuple = df.pairOf(1.0, 2.0);
        assertNotNull(tuple);
        assert (tuple.getItemOne() == 1.0);
        assert (tuple.getItemTwo() == 2.0);
    }
}
