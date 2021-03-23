package wres.systests;

import static org.junit.Assert.assertEquals;

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

import wres.control.Control;
public class Scenario652
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Scenario652.class );
    private static final String NEWLINE = System.lineSeparator();

    /**
     * Expected paths as file names.
     */

    private static final Set<Path> EXPECTED_FILE_NAMES =
            Set.of( Path.of( "124.750175W_50.542873N_NWM_Short_Range_MEAN_ERROR.csv" ),
                    Path.of( "124.750175W_50.542873N_NWM_Short_Range_PEARSON_CORRELATION_COEFFICIENT.csv" ),
                    Path.of( "124.750175W_50.542873N_NWM_Short_Range_SAMPLE_SIZE.csv" ),
                    Path.of( "124.75169W_50.463585N_NWM_Short_Range_MEAN_ERROR.csv" ),
                    Path.of( "124.75169W_50.463585N_NWM_Short_Range_PEARSON_CORRELATION_COEFFICIENT.csv" ),
                    Path.of( "124.75169W_50.463585N_NWM_Short_Range_SAMPLE_SIZE.csv" ),
                    Path.of( "124.752846W_50.492928N_NWM_Short_Range_MEAN_ERROR.csv" ),
                    Path.of( "124.752846W_50.492928N_NWM_Short_Range_PEARSON_CORRELATION_COEFFICIENT.csv" ),
                    Path.of( "124.752846W_50.492928N_NWM_Short_Range_SAMPLE_SIZE.csv" ),
                    Path.of( "124.75399W_50.522266N_NWM_Short_Range_MEAN_ERROR.csv" ),
                    Path.of( "124.75399W_50.522266N_NWM_Short_Range_PEARSON_CORRELATION_COEFFICIENT.csv" ),
                    Path.of( "124.75399W_50.522266N_NWM_Short_Range_SAMPLE_SIZE.csv" ),
                    Path.of( "124.75665W_50.472317N_NWM_Short_Range_MEAN_ERROR.csv" ),
                    Path.of( "124.75665W_50.472317N_NWM_Short_Range_PEARSON_CORRELATION_COEFFICIENT.csv" ),
                    Path.of( "124.75665W_50.472317N_NWM_Short_Range_SAMPLE_SIZE.csv" ),
                    Path.of( "124.7578W_50.501656N_NWM_Short_Range_MEAN_ERROR.csv" ),
                    Path.of( "124.7578W_50.501656N_NWM_Short_Range_PEARSON_CORRELATION_COEFFICIENT.csv" ),
                    Path.of( "124.7578W_50.501656N_NWM_Short_Range_SAMPLE_SIZE.csv" ),
                    Path.of( "124.75895W_50.530994N_NWM_Short_Range_MEAN_ERROR.csv" ),
                    Path.of( "124.75895W_50.530994N_NWM_Short_Range_PEARSON_CORRELATION_COEFFICIENT.csv" ),
                    Path.of( "124.75895W_50.530994N_NWM_Short_Range_SAMPLE_SIZE.csv" ),
                    Path.of( "124.76045W_50.451702N_NWM_Short_Range_MEAN_ERROR.csv" ),
                    Path.of( "124.76045W_50.451702N_NWM_Short_Range_PEARSON_CORRELATION_COEFFICIENT.csv" ),
                    Path.of( "124.76045W_50.451702N_NWM_Short_Range_SAMPLE_SIZE.csv" ),
                    Path.of( "124.761604W_50.481045N_NWM_Short_Range_MEAN_ERROR.csv" ),
                    Path.of( "124.761604W_50.481045N_NWM_Short_Range_PEARSON_CORRELATION_COEFFICIENT.csv" ),
                    Path.of( "124.761604W_50.481045N_NWM_Short_Range_SAMPLE_SIZE.csv" ),
                    Path.of( "124.76276W_50.510384N_NWM_Short_Range_MEAN_ERROR.csv" ),
                    Path.of( "124.76276W_50.510384N_NWM_Short_Range_PEARSON_CORRELATION_COEFFICIENT.csv" ),
                    Path.of( "124.76276W_50.510384N_NWM_Short_Range_SAMPLE_SIZE.csv" ),
                    Path.of( "124.76391W_50.539722N_NWM_Short_Range_MEAN_ERROR.csv" ),
                    Path.of( "124.76391W_50.539722N_NWM_Short_Range_PEARSON_CORRELATION_COEFFICIENT.csv" ),
                    Path.of( "124.76391W_50.539722N_NWM_Short_Range_SAMPLE_SIZE.csv" ),
                    Path.of( "124.76541W_50.460434N_NWM_Short_Range_MEAN_ERROR.csv" ),
                    Path.of( "124.76541W_50.460434N_NWM_Short_Range_PEARSON_CORRELATION_COEFFICIENT.csv" ),
                    Path.of( "124.76541W_50.460434N_NWM_Short_Range_SAMPLE_SIZE.csv" ),
                    Path.of( "124.76656W_50.489773N_NWM_Short_Range_MEAN_ERROR.csv" ),
                    Path.of( "124.76656W_50.489773N_NWM_Short_Range_PEARSON_CORRELATION_COEFFICIENT.csv" ),
                    Path.of( "124.76656W_50.489773N_NWM_Short_Range_SAMPLE_SIZE.csv" ),
                    Path.of( "124.767715W_50.51911N_NWM_Short_Range_MEAN_ERROR.csv" ),
                    Path.of( "124.767715W_50.51911N_NWM_Short_Range_PEARSON_CORRELATION_COEFFICIENT.csv" ),
                    Path.of( "124.767715W_50.51911N_NWM_Short_Range_SAMPLE_SIZE.csv" ),
                    Path.of( "124.768875W_50.54845N_NWM_Short_Range_MEAN_ERROR.csv" ),
                    Path.of( "124.768875W_50.54845N_NWM_Short_Range_PEARSON_CORRELATION_COEFFICIENT.csv" ),
                    Path.of( "124.768875W_50.54845N_NWM_Short_Range_SAMPLE_SIZE.csv" ),
                    Path.of( "pairs.csv" ),
                    Path.of( "NWM_Short_Range_20180526T040000Z_10800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180526T040000Z_14400_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180526T040000Z_3600_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180526T040000Z_7200_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180526T080000Z_10800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180526T080000Z_14400_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180526T080000Z_3600_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180526T080000Z_7200_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180526T120000Z_10800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180526T120000Z_14400_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180526T120000Z_3600_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180526T120000Z_7200_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180526T160000Z_10800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180526T160000Z_14400_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180526T160000Z_3600_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180526T160000Z_7200_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180526T200000Z_10800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180526T200000Z_14400_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180526T200000Z_3600_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180526T200000Z_7200_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180527T000000Z_10800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180527T000000Z_14400_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180527T000000Z_3600_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180527T000000Z_7200_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180527T040000Z_10800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180527T040000Z_14400_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180527T040000Z_3600_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180527T040000Z_7200_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180527T080000Z_10800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180527T080000Z_14400_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180527T080000Z_3600_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180527T080000Z_7200_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180527T120000Z_10800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180527T120000Z_14400_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180527T120000Z_3600_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180527T120000Z_7200_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180527T160000Z_10800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180527T160000Z_14400_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180527T160000Z_3600_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180527T160000Z_7200_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180527T200000Z_10800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180527T200000Z_14400_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180527T200000Z_3600_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180527T200000Z_7200_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180528T000000Z_10800_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180528T000000Z_14400_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180528T000000Z_3600_SECONDS.nc" ),
                    Path.of( "NWM_Short_Range_20180528T000000Z_7200_SECONDS.nc" ) );

    
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
        Set<Path> pathsWritten = ScenarioHelper.executeScenario( scenarioInfo );
        Set<Path> actualFileNamesThatExist = pathsWritten.stream()
                                                         .filter( Files::exists )
                                                         .map( Path::getFileName )
                                                         .collect( Collectors.toSet() );

        // Expected file-name paths equals actual
        LOGGER.info( "Checking expected file names against actual file names that exist for {} files...",
                     EXPECTED_FILE_NAMES.size() );

        assertEquals( "The actual set of file names does not match the expected set of file names.",
                      EXPECTED_FILE_NAMES,
                      actualFileNamesThatExist );
        
        LOGGER.info( "Finished checking file names. The actual file names match the expected file names." );
        
        ScenarioHelper.assertOutputsMatchBenchmarks( scenarioInfo, pathsWritten );
        LOGGER.info( "########################################################## COMPLETED "
                + this.getClass().getSimpleName().toLowerCase() + NEWLINE);
    }
}

