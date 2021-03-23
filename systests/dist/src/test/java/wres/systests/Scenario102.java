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

public class Scenario102
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Scenario102.class );
    private static final String NEWLINE = System.lineSeparator();

    /**
     * Expected paths as file names.
     */

    private static final Set<Path> EXPECTED_FILE_NAMES =
            Set.of( Path.of( "LGNN5_LGNN5_ESP_BIAS_FRACTION.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_151200_SECONDS.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_151200_SECONDS.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_BOX_PLOT_OF_ERRORS.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_BOX_PLOT_OF_PERCENTAGE_ERRORS.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_BRIER_SCORE.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_BRIER_SKILL_SCORE.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_COEFFICIENT_OF_DETERMINATION.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_CONTINUOUS_RANKED_PROBABILITY_SCORE.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_INDEX_OF_AGREEMENT.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_KLING_GUPTA_EFFICIENCY.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_MAXIMUM.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_MEAN_ABSOLUTE_ERROR.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_MEAN.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_MEAN_ERROR.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_MEAN_SQUARE_ERROR.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_MEAN_SQUARE_ERROR_SKILL_SCORE.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_MEAN_SQUARE_ERROR_SKILL_SCORE_NORMALIZED.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_MEDIAN_ERROR.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_MINIMUM.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_PEARSON_CORRELATION_COEFFICIENT.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_QUANTILE_QUANTILE_DIAGRAM_151200_SECONDS.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_RANK_HISTOGRAM_151200_SECONDS.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_151200_SECONDS.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_RELATIVE_OPERATING_CHARACTERISTIC_SCORE.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_RELIABILITY_DIAGRAM_151200_SECONDS.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_ROOT_MEAN_SQUARE_ERROR.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_ROOT_MEAN_SQUARE_ERROR_NORMALIZED.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_SAMPLE_SIZE.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_STANDARD_DEVIATION.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_SUM_OF_SQUARE_ERROR.csv" ),
                    Path.of( "LGNN5_LGNN5_ESP_VOLUMETRIC_EFFICIENCY.csv" ),
                    Path.of( "pairs.csv" ) );
    
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

