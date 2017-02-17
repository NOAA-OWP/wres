/**
 * 
 */
package wres.configcontrol.config.project;

//Imports
import java.net.*;
import java.util.*;
import org.junit.*;
import com.google.common.testing.*;

//WRES dependencies
import wres.configcontrol.config.*;

/**
 * Test class for {@link DataIdentifierSet} 
 * 
 * @author james.brown@hydrosolved.com
 */
public class DataIdentifierSetTest {

	/**
	 * Tests for equality of two {@link DataIdentifierSet}.
	 */	
	@Test
	public void assertEqual() {
		Map<Integer,String> a = new TreeMap<>();
		a.put(Identifier.OBJECT_IDENTIFIER,"ORDC1"); 
		a.put(Identifier.ATTRIBUTE_IDENTIFIER,"SQIN"); 
		a.put(Identifier.SCENARIO_IDENTIFIER,"GEFS"); 

		Map<Integer,String> b = new TreeMap<>();
		b.put(Identifier.OBJECT_IDENTIFIER,"ORDC1"); 
		b.put(Identifier.ATTRIBUTE_IDENTIFIER,"SQIN"); 
		b.put(Identifier.SCENARIO_IDENTIFIER,"GEFS"); 		

		Map<Integer,String> c = new TreeMap<>();
		c.put(Identifier.OBJECT_IDENTIFIER,"ORDC1"); 
		c.put(Identifier.ATTRIBUTE_IDENTIFIER,"SQIN"); 
		
		Identifier id1 = new Identifier(a);
		Identifier id2 = new Identifier(b);
		Identifier id3 = new Identifier(c);
		URI u1 = null;
		URI u2 = null;
		URI u3 = null;
		try {
			 u1 = new URI("some.location");
			 u2 = new URI("some.location");
			 u3 = new URI("some.other.location");
		} catch(Exception e) {
			Assert.fail("Test failed : " + e.getMessage());
		}
		DataIdentifier p = new DataIdentifier(id1,u1);
		DataIdentifier q = new DataIdentifier(id2,u2);
		DataIdentifier r = new DataIdentifier(id3,u3);
		
		DataIdentifierSet x = new DataIdentifierSet();
		x.fluentAdd(p).fluentAdd(q).fluentAdd(r);
		DataIdentifierSet y = x.deepCopy();
		
		Map<Integer,String> d = new TreeMap<>();
		d.put(Identifier.OBJECT_IDENTIFIER,"FCFM8"); 
		d.put(Identifier.ATTRIBUTE_IDENTIFIER,"SQIN"); 
		d.put(Identifier.SCENARIO_IDENTIFIER,"GEFS"); 		
		Identifier id4 = new Identifier(a);
		DataIdentifier s = new DataIdentifier(id4,u1);
		DataIdentifierSet z = new DataIdentifierSet();
		z.add(s);
		//Test
		new EqualsTester()
	     .addEqualityGroup(x, y)
	     .addEqualityGroup(z)
	     .testEquals();
	}		
	

}
