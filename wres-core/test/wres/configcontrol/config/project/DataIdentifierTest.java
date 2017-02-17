/**
 * 
 */
package wres.configcontrol.config.project;

//Imports
import java.util.*;
import java.net.*;
import org.junit.*;
import com.google.common.testing.*;

//WRES dependencies
import wres.configcontrol.config.*;

/**
 * Test class for {@link DataIdentifier} 
 * 
 * @author james.brown@hydrosolved.com
 */
public class DataIdentifierTest {

	/**
	 * Tests for equality of two {@link DataIdentifier}.
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
		//Test
		new EqualsTester()
	     .addEqualityGroup(p, q)
	     .addEqualityGroup(r)
	     .testEquals();
	}	
	
	/**
	 * Tests the {@link Comparable#compareTo(Object)} methods of two {@link DataIdentifier}.
	 */	
	@Test
	public void assertComparable() {    
		Map<Integer,String> a = new TreeMap<>();
		a.put(Identifier.OBJECT_IDENTIFIER,"ORDC1"); 
		a.put(Identifier.ATTRIBUTE_IDENTIFIER,"SQIN"); 
		a.put(Identifier.SCENARIO_IDENTIFIER,"GEFS"); 

		Map<Integer,String> b = new TreeMap<>();
		b.put(Identifier.OBJECT_IDENTIFIER,"ORDC1"); 
		b.put(Identifier.ATTRIBUTE_IDENTIFIER,"SQIN"); 
		b.put(Identifier.SCENARIO_IDENTIFIER,"GEFS"); 		
		
		Identifier id1 = new Identifier(a);
		Identifier id2 = new Identifier(b);
		URI u1 = null;
		URI u2 = null;
		try {
			 u1 = new URI("some.location");
			 u2 = new URI("some.location");
		} catch(Exception e) {
			Assert.fail("Test failed : " + e.getMessage());
		}		
		DataIdentifier p = new DataIdentifier(id1,u1);
		DataIdentifier q = new DataIdentifier(id2,u2);
		String msgA = "Objects were not comparable: <" + p + ">, <" + q + ">";
		
		Assert.assertTrue(msgA,p.compareTo(q)==0);
		Assert.assertTrue(msgA,q.compareTo(p)==0);
		
		Map<Integer,String> c = new TreeMap<>();
		c.put(Identifier.OBJECT_IDENTIFIER,"ORDC1"); 
		c.put(Identifier.ATTRIBUTE_IDENTIFIER,"SQIN"); 
		URI u3 = null;
		try {
			 u3 = new URI("some.other.location");
		} catch(Exception e) {
			Assert.fail("Test failed : " + e.getMessage());
		}
		Identifier id3 = new Identifier(c);
		DataIdentifier r = new DataIdentifier(id3,u3);
		String msgB = "Comparison failed: <" + p + ">, <" + r + ">";
		Assert.assertTrue(msgB,p.compareTo(r)>0);
		Assert.assertTrue(msgB,r.compareTo(p)<0);
	}	
	

}
