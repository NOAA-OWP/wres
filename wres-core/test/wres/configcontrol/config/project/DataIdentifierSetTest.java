/**
 * 
 */
package wres.configcontrol.config.project;

// Imports
import java.net.URI;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.testing.EqualsTester;

// WRES dependencies
import wres.configcontrol.config.Identifier;

/**
 * Test class for {@link DataIdentifierSet}
 * 
 * @author james.brown@hydrosolved.com
 */
public class DataIdentifierSetTest
{

    /**
     * Tests for equality of two {@link DataIdentifierSet}.
     */
    @Test
    public void assertEqual()
    {
        final Map<Integer, String> a = new TreeMap<>();
        a.put(Identifier.OBJECT_IDENTIFIER, "ORDC1");
        a.put(Identifier.ATTRIBUTE_IDENTIFIER, "SQIN");
        a.put(Identifier.SCENARIO_IDENTIFIER, "GEFS");

        final Map<Integer, String> b = new TreeMap<>();
        b.put(Identifier.OBJECT_IDENTIFIER, "ORDC1");
        b.put(Identifier.ATTRIBUTE_IDENTIFIER, "SQIN");
        b.put(Identifier.SCENARIO_IDENTIFIER, "GEFS");

        final Map<Integer, String> c = new TreeMap<>();
        c.put(Identifier.OBJECT_IDENTIFIER, "ORDC1");
        c.put(Identifier.ATTRIBUTE_IDENTIFIER, "SQIN");

        final Identifier id1 = new Identifier(a);
        final Identifier id2 = new Identifier(b);
        final Identifier id3 = new Identifier(c);
        URI u1 = null;
        URI u2 = null;
        URI u3 = null;
        try
        {
            u1 = new URI("some.location");
            u2 = new URI("some.location");
            u3 = new URI("some.other.location");
        }
        catch(final Exception e)
        {
            Assert.fail("Test failed : " + e.getMessage());
        }
        final DataIdentifier p = new DataIdentifier(id1, u1);
        final DataIdentifier q = new DataIdentifier(id2, u2);
        final DataIdentifier r = new DataIdentifier(id3, u3);

        final DataIdentifierSet x = new DataIdentifierSet();
        x.fluentAdd(p).fluentAdd(q).fluentAdd(r);
        final DataIdentifierSet y = x.deepCopy();

        final Map<Integer, String> d = new TreeMap<>();
        d.put(Identifier.OBJECT_IDENTIFIER, "FCFM8");
        d.put(Identifier.ATTRIBUTE_IDENTIFIER, "SQIN");
        d.put(Identifier.SCENARIO_IDENTIFIER, "GEFS");
        final Identifier id4 = new Identifier(a);
        final DataIdentifier s = new DataIdentifier(id4, u1);
        final DataIdentifierSet z = new DataIdentifierSet();
        z.add(s);
        //Test
        new EqualsTester().addEqualityGroup(x, y).addEqualityGroup(z).testEquals();
    }

}
