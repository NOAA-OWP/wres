package wres.datamodel;

import org.junit.Test;
import static org.junit.Assert.*;

public class FactoryBindingTest
{
    @Test
    public void useDataFactoryTest()
    {
        TupleOfDoubles tuple = DataFactory.tupleOf(1.0, 2.0);
        assertNotNull(tuple);
        assertNotNull(tuple.getTupleOfDoubles());
        assertNotNull(tuple.getTupleOfDoubles()[0]);
        assertNotNull(tuple.getTupleOfDoubles()[1]);
        assert(tuple.getTupleOfDoubles()[0] == 1.0);
        assert(tuple.getTupleOfDoubles()[1] == 2.0);
    }
}
