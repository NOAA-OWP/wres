/**
 * 
 */
package wres.configcontrol.config.project;

// Imports
import java.io.File;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.junit.Assert;
import org.junit.Test;

import wres.configcontrol.config.Identifier;

/**
 * Test class for {@link VerificationProject}
 * 
 * @author james.brown@hydrosolved.com
 */
public class VerificationProjectTest
{

    /**
     * Test the marshalling of a verification project to an XML file in the test output directory.
     */
    @Test
    public void test1MarshalVerificationProject()
    {
        try
        {
            //Construct the marshaller
            final JAXBContext jaxbContext = JAXBContext.newInstance(VerificationProject.class);
            final Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
            jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            //Construct the test project
            final VerificationProject p = new VerificationProject(new Identifier(Identifier.CONFIGURATION_IDENTIFIER,
                                                                                 "firstProject"));
            //Marshal the project to an output file
            final File f = new File("testoutput/wres/configcontrol/config/project/verificationProjectTest/test1.xml");
            jaxbMarshaller.marshal(p, f);

            //Compare for equality with the benchmark

        }
        catch(final Exception e)
        {
            e.printStackTrace();
            Assert.fail("Test failed : " + e.getMessage());
        }

    }

    /**
     * Test the unmarshalling of a verification project from an XML file in the test input directory.
     */
    @Test
    public void test2UnmarshalVerificationProject()
    {
        try
        {

            //Create the input file
            final File f = new File("testinput/wres/configcontrol/config/project/verificationProjectTest/test1.xml");

            //Construct the unmarshaller
            final JAXBContext jaxbContext = JAXBContext.newInstance(VerificationProject.class);
            final Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
            final VerificationProject p = (VerificationProject)jaxbUnmarshaller.unmarshal(f);

            //Compare for equality with the benchmark
            final VerificationProject q = new VerificationProject(new Identifier(Identifier.CONFIGURATION_IDENTIFIER,
                                                                                 "firstProject"));

        }
        catch(final Exception e)
        {
            e.printStackTrace();
            Assert.fail("Test failed : " + e.getMessage());
        }

    }

}
