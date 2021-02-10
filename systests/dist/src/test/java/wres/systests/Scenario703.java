package wres.systests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import wres.control.Control;

public class Scenario703
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Scenario703.class );
    private static final String NEWLINE = System.lineSeparator();

    // There are 8 metrics and we expect at least 300 features.
    private static final int EXPECTED_AT_LEAST_THIS_MANY_CSV_FILES = 8 * 300;

    /**
     * A set of netCDF file names to always expect
     */

    private static final Set<Path> EXPECTED_NC_FILES =
            Set.of( Path.of( "NWM_Short_Range_20170808T000000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T010000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T020000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T030000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T040000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T050000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T060000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T070000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T080000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T090000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T100000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T110000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T120000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T130000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T140000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T150000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T160000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T170000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T180000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T190000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T200000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T210000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T220000Z_64800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20170808T230000Z_64800_SECONDS.nc" ) );


    private ScenarioInformation scenarioInfo;

    /**
     * Watch for any failed assertions and log them.
     */

    @Rule
    public TestWatcher watcher = new TestWatcher()
    {
        @Override
        protected void failed( Throwable e, Description description )
        {
            LOGGER.error( description.toString(), e );
        }
    };

    @Before
    public void beforeIndividualTest() throws IOException, SQLException
    {
        LOGGER.info( "########################################################## EXECUTING "
                     + this.getClass().getSimpleName().toLowerCase()
                     + NEWLINE );
        this.scenarioInfo = new ScenarioInformation( this.getClass()
                                                         .getSimpleName()
                                                         .toLowerCase(),
                                                     ScenarioHelper.getBaseDirectory() );
        ScenarioHelper.logUsedSystemProperties( scenarioInfo );
    }

    @Test
    public void testScenario()
    {
        Control control = ScenarioHelper.assertExecuteScenario( scenarioInfo );

        // Collect the file names actually written and that exist
        Set<Path> pathsWritten = control.get();
        Set<Path> actualFileNamesThatExist = pathsWritten.stream()
                                                         .filter( Files::exists )
                                                         .map( Path::getFileName )
                                                         .collect( Collectors.toSet() );
        Set<Path> netcdfFilesThatExist =
                actualFileNamesThatExist.stream()
                                        .filter( p -> p.toString()
                                                       .endsWith( ".nc" ) )
                                        .collect( Collectors.toSet() );
        Set<Path> csvFilesThatExist =
                actualFileNamesThatExist.stream()
                                        .filter( p -> p.toString()
                                                       .endsWith( ".csv" ) )
                                        .collect( Collectors.toSet() );
        assertEquals( "The actual set of netCDF file names does not match the expected set of file names."
                      + " These existed in expected, but not in actual: "
                      + Sets.difference( EXPECTED_NC_FILES, netcdfFilesThatExist )
                      + " while these existed in actual, but not expected: "
                      + Sets.difference( netcdfFilesThatExist, EXPECTED_NC_FILES ),
                      EXPECTED_NC_FILES,
                      netcdfFilesThatExist );

        // Because NWIS adds and removes sites in this set every so often, this
        // test will do broad-strokes assertions rather than precise ones. The
        // correctness of each WRES component involved is handled elsewhere.
        int countOfCsvFiles = csvFilesThatExist.size();
        assertTrue( "The count of CSV files was expected to be above "
                    + EXPECTED_AT_LEAST_THIS_MANY_CSV_FILES + " but was "
                    + countOfCsvFiles,
                    countOfCsvFiles > EXPECTED_AT_LEAST_THIS_MANY_CSV_FILES );
        assertTrue( "The count of non-pairs CSV files was expected to be divisible by 8 but was not: "
                    + ( countOfCsvFiles - 1 ),
                    ( countOfCsvFiles - 1 ) % 8 == 0 );
        LOGGER.info( "########################################################## COMPLETED "
                     + this.getClass().getSimpleName().toLowerCase()
                     + NEWLINE );
    }
}
