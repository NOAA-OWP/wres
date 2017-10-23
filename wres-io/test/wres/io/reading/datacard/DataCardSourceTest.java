package wres.io.reading.datacard;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DataSourceConfig.Source;
import wres.config.generated.DataSourceConfig.Variable;
import wres.config.generated.Feature;
import wres.config.generated.Format;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.io.data.caching.DataSources;
import wres.io.data.details.ProjectDetails;


@RunWith( PowerMockRunner.class )
@PrepareForTest( { DataSources.class } )
@PowerMockIgnore( "javax.management.*") // thanks https://stackoverflow.com/questions/16520699/mockito-powermock-linkageerror-while-mocking-system-class#21268013
public class DataCardSourceTest
{

    private static final String EOL = System.lineSeparator();

	private DatacardSource source = null;
	private static final String EXPECTED_QUERY_NORMAL = "123|'1960-01-01T19:00Z'|201.0|456|789" + EOL +
														 "123|'1960-01-02T19:00Z'|181.0|456|789" + EOL +
														 "123|'1960-01-03T19:00Z'|243.0|456|789" + EOL +
														 "123|'1960-01-04T19:00Z'|409.0|456|789" + EOL +
														 "123|'1960-01-05T19:00Z'|364.0|456|789" + EOL +
														 "123|'1960-01-06T19:00Z'|306.0|456|789" + EOL +
														 "123|'1960-01-07T19:00Z'|268.0|456|789" + EOL +
														 "123|'1960-01-08T19:00Z'|240.0|456|789" + EOL +
														 "123|'1960-01-09T19:00Z'|220.0|456|789" + EOL +
														 "123|'1960-01-10T19:00Z'|198.0|456|789";

    // By hand, computed the first value should be "1949-01-01T11:00" UTC
    // by using the guide here:
    // http://www.nws.***REMOVED***/oh/hrl/nwsrfs/users_manual/part7/_pdf/72datacard.pdf
    // plus Hank's guidance:
    //  "datacard files work on a 1-24 clock, not 0-23.  In other words, hour 0
    //   is actually thought of as hour 24 of the previous day. ...
    //   Thus, the first value for a given month is actually *one time step
    //   into that month*"
	private static final String EXPECTED_QUERY_SHORT_RECORD =
            "123|'1949-01-01T11:00'|0.65|456|789" + EOL
            + "123|'1949-01-01T17:00'|0.229|456|789" + EOL
            + "123|'1949-01-01T23:00'|0.0|456|789" + EOL
            + "123|'1949-01-02T05:00'|0.024|456|789" + EOL
            + "123|'1949-01-02T11:00'|0.025|456|789" + EOL
            + "123|'1949-01-02T17:00'|0.001|456|789" + EOL
            + "123|'1949-01-02T23:00'|0.0|456|789" + EOL
            + "123|'1949-01-03T05:00'|0.0|456|789" + EOL
            + "123|'1949-01-03T11:00'|0.0|456|789" + EOL
            + "123|'1949-01-03T17:00'|0.0|456|789" + EOL
            + "123|'1949-01-03T23:00'|0.0|456|789" + EOL
            + "123|'1949-01-04T05:00'|0.001|456|789" + EOL
            + "123|'1949-01-04T11:00'|0.0|456|789" + EOL
            + "123|'1949-01-04T17:00'|0.015|456|789" + EOL
            + "123|'1949-01-05T05:00'|0.028|456|789" + EOL
            + "123|'1949-01-05T11:00'|0.526|456|789" + EOL
            + "123|'1949-01-05T17:00'|0.263|456|789" + EOL
            + "123|'1949-01-05T23:00'|0.188|456|789" + EOL
            + "123|'1949-01-06T05:00'|0.022|456|789" + EOL
            + "123|'1949-01-06T11:00'|0.0|456|789" + EOL
            + "123|'1949-01-06T17:00'|0.0|456|789";

	@Mock DataSources mockDataSources;
	@Mock ProjectDetails mockProjectDetails;

	@Before
    public void setup() throws Exception
    {
        PowerMockito.whenNew( DataSources.class )
                    .withAnyArguments()
                    .thenReturn( mockDataSources );

        PowerMockito.whenNew( ProjectDetails.class )
                    .withAnyArguments()
                    .thenReturn( mockProjectDetails );

        when(mockDataSources.hasID( any() ) ).thenReturn( true );
        when(mockDataSources.getID( any() ) ).thenReturn( 0 );
    }

	@Test
    @Ignore // TODO: revisit this test and determine appropriate output
    public void insertQueryNormalTest()
			throws IOException
    {
        String current = new java.io.File( "." ).getCanonicalPath();
        List<DataSourceConfig.Source> sourceList = new ArrayList<DataSourceConfig.Source>();
        Format f = Format.fromValue("datacard");

        DataSourceConfig.Source confSource = new Source(current, f, "IN", "DRRC2",
                                                        "EST", "-999.0", true,
                                                        true, "-998.0", "$");

        sourceList.add(confSource);

        DataSourceConfig config = new DataSourceConfig(DatasourceType.fromValue("observations"),
                                                       sourceList,
                                                       new Variable("QINE", "test", "IN"),
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null);

        ProjectConfig.Inputs inputs = new ProjectConfig.Inputs( config,
                                                                config,
                                                                null );

        Feature featureConfig = new Feature( null,
                                             null,
                                             "FAKELID",
                                             null,
                                             null,
                                             null,
                                             null,
                                             null );

        List<Feature> features = new ArrayList<>();
        features.add( featureConfig );
        PairConfig pairConfig = new PairConfig( "CMS",
                                                features,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null );

        ProjectConfig projectConfig = new ProjectConfig( inputs,
                                                         pairConfig,
                                                         null,
                                                         null,
                                                         null,
                                                         null );

        source = new wres.io.reading.datacard.DatacardSource(current + "/testinput/datacard/short_HOPR1SNE.QME.OBS");
        source.setProjectDetails( new ProjectDetails( projectConfig ) );source.setDataSourceConfig(config);
        source.setTestMode(true);

        source.setVariablePositionID(123);
        source.setMeasurementID(456);
        source.setSourceID(789);
        source.saveObservation();

        String query = source.getInsertQuery();
        assertEquals( "Expected equal outputs.", EXPECTED_QUERY_NORMAL, query );

	}
	
	@Test
	//Test short record and multiple specified missing values
    public void insertQueryShortRecordTest()
            throws IOException
    {
        String current = new java.io.File( "." ).getCanonicalPath();
        List<DataSourceConfig.Source> sourceList = new ArrayList<DataSourceConfig.Source>();
        Format f = Format.fromValue("datacard");

        DataSourceConfig.Source confSource = new Source(current, f, "IN", "DRRC2",
                                                        "EST","-999.0, -997", true,
                                                        true, "-998.0", "$");

        sourceList.add(confSource);

        DataSourceConfig config = new DataSourceConfig(DatasourceType.fromValue("observations"),
                                                       sourceList,
                                                       new Variable("QINE", "test", "IN"),
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null);

        ProjectConfig.Inputs inputs = new ProjectConfig.Inputs( config,
                                                                config,
                                                                null );
        Feature featureConfig = new Feature( null,
                                             null,
                                             "FAKELID",
                                             null,
                                             null,
                                             null,
                                             null,
                                             null );

        List<Feature> features = new ArrayList<>();
        features.add( featureConfig );
        PairConfig pairConfig = new PairConfig( "CMS",
                                                features,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null );

        ProjectConfig projectConfig = new ProjectConfig( inputs,
                                                         pairConfig,
                                                         null,
                                                         null,
                                                         null,
                                                         null );
        source = new wres.io.reading.datacard.DatacardSource(current + "/testinput/datacard/short_CCRN6.MAP06_short_record");

        source.setProjectDetails( new ProjectDetails( projectConfig ) );
        source.setDataSourceConfig(config);
        source.setTestMode(true);

        source.setVariablePositionID(123);
        source.setMeasurementID(456);
        source.setSourceID(789);
        source.saveObservation();

        String query = source.getInsertQuery();
        assertEquals( "Expected equal outputs.", EXPECTED_QUERY_SHORT_RECORD, query );
	}

}
