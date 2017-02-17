/**
 * 
 */
package wres.configcontrol.config.project;

// Imports
import java.io.*;
import org.junit.*;
import javax.xml.bind.*;
import wres.configcontrol.config.*;

/**
 * Test class for {@link VerificationProject}
 * 
 * @author james.brown@hydrosolved.com
 */
public class VerificationProjectTest {

	/**
	 * Test the marshalling of a verification project to an XML file in the test output directory.
	 */
	@Test
	public void test1MarshalVerificationProject() {
		try {
			//Construct the marshaller
			JAXBContext jaxbContext = JAXBContext.newInstance(VerificationProject.class);
			Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
			jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
			//Construct the test project
			VerificationProject p = new VerificationProject(
					new Identifier(Identifier.CONFIGURATION_IDENTIFIER, "firstProject"));
			//Marshal the project to an output file
			File f = new File("testoutput/wres/configcontrol/config/project/verificationProjectTest/test1.xml");
			jaxbMarshaller.marshal(p,f);

			//Compare for equality with the benchmark
			
			

		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test failed : " + e.getMessage());
		}

	}

	/**
	 * Test the unmarshalling of a verification project from an XML file in the test input directory.
	 */
	@Test
	public void test2UnmarshalVerificationProject() {
		try {
			
			//Create the input file
			File f = new File("testdata/wres/configcontrol/config/project/verificationProjectTest/test1.xml");
			
			//Construct the unmarshaller
			JAXBContext jaxbContext = JAXBContext.newInstance(VerificationProject.class);
			Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
			VerificationProject p = (VerificationProject)jaxbUnmarshaller.unmarshal(f);
			
			//Compare for equality with the benchmark
			VerificationProject q = new VerificationProject(
					new Identifier(Identifier.CONFIGURATION_IDENTIFIER, "firstProject"));
			

		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test failed : " + e.getMessage());
		}

	}	
	
}
