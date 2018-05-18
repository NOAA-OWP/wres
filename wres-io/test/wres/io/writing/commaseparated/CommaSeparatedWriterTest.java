package wres.io.writing.commaseparated;

import java.util.ArrayList;
import java.util.List;

import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.Feature;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;

// uncomment these if we figure out what was wrong with powermockito setup
//@RunWith( PowerMockRunner.class )
//@PrepareForTest( { Files.class, BufferedWriter.class, Writer.class } )
//@PowerMockIgnore( "javax.management.*" )
public class CommaSeparatedWriterTest
{

// what follows is a started attempt at avoiding filesystem (use powermockito)
//
//    StringWriter outputString;
//    BufferedWriter bufferedWriter;
//
//    @Before
//    public void setup() throws Exception //yuck, but that's sig of whenNew
//    {
//        // Fake out the file that is written, write to String instead
//        outputString = new StringWriter();
//        bufferedWriter = new BufferedWriter( outputString );
//
//        PowerMockito.mockStatic( Files.class );
//        //PowerMockito.mockStatic( BufferedWriter.class );
//
//        // does not work:
//        PowerMockito.whenNew( BufferedWriter.class )
//                    .withAnyArguments()
//                    .thenReturn( bufferedWriter );
//
//        // does not work:
//        PowerMockito.whenNew( BufferedWriter.class )
//                    .withArguments( Writer.class )
//                    .thenReturn( bufferedWriter );
//
//        // does not work:
//        PowerMockito.whenNew( BufferedWriter.class )
//                    .withArguments( any() )
//                    .thenReturn( bufferedWriter );
//
//        // does not work:
//        PowerMockito.whenNew( BufferedWriter.class )
//                    .withArguments( any(Writer.class), anyInt() )
//                    .thenReturn( bufferedWriter );
//
//    }

    /**
     * Returns a fake project configuration for a specified feature.
     * 
     * @param feature the feature
     * @return fake project configuration
     */

    ProjectConfig getMockedProjectConfig( Feature feature )
    {
        // Use the system temp directory so that checks for writeability pass.
        DestinationConfig destinationConfig =
                new DestinationConfig( System.getProperty( "java.io.tmpdir" ),
                                       null,
                                       null,
                                       null,
                                       DestinationType.NUMERIC,
                                       null );

        List<DestinationConfig> destinations = new ArrayList<>();
        destinations.add( destinationConfig );

        ProjectConfig.Outputs outputsConfig =
                new ProjectConfig.Outputs( destinations );

        List<Feature> features = new ArrayList<>();
        features.add( feature );

        PairConfig pairConfig = new PairConfig( null,
                                                features,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null );

        ProjectConfig projectConfig = new ProjectConfig( null,
                                                         pairConfig,
                                                         null,
                                                         outputsConfig,
                                                         null,
                                                         "test" );
        return projectConfig;
    }

    /**
     * Returns a fake feature for a specified location identifier.
     * 
     * @param locationId the location identifier
     */

    Feature getMockedFeature( String locationId )
    {
        return new Feature( null,
                            null,
                            null,
                            null,
                            null,
                            locationId,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null );
    }

}
