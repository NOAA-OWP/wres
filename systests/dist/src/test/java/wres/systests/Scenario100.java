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

public class Scenario100
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Scenario100.class );
    private static final String NEWLINE = System.lineSeparator();

    /**
     * Expected paths as file names.
     */

    private static final Set<Path> EXPECTED_FILE_NAMES =
            Set.of( Path.of( "LGNN5_LGNN5_HEFS_BIAS_FRACTION.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_BIAS_FRACTION.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_BOX_PLOT_OF_ERRORS.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_BOX_PLOT_OF_ERRORS.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_151200_SECONDS.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_151200_SECONDS.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_151200_SECONDS.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_151200_SECONDS.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_BOX_PLOT_OF_PERCENTAGE_ERRORS.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_BOX_PLOT_OF_PERCENTAGE_ERRORS.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_BRIER_SCORE.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_BRIER_SCORE.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_BRIER_SKILL_SCORE.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_BRIER_SKILL_SCORE.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_COEFFICIENT_OF_DETERMINATION.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_COEFFICIENT_OF_DETERMINATION.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_CONTINGENCY_TABLE.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_CONTINUOUS_RANKED_PROBABILITY_SCORE.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_CONTINUOUS_RANKED_PROBABILITY_SCORE.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_ENSEMBLE_QUANTILE_QUANTILE_DIAGRAM_151200_SECONDS.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_ENSEMBLE_QUANTILE_QUANTILE_DIAGRAM_151200_SECONDS.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_EQUITABLE_THREAT_SCORE.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_EQUITABLE_THREAT_SCORE.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_FALSE_ALARM_RATIO.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_FALSE_ALARM_RATIO.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_FREQUENCY_BIAS.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_FREQUENCY_BIAS.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_INDEX_OF_AGREEMENT.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_INDEX_OF_AGREEMENT.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_KLING_GUPTA_EFFICIENCY.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_KLING_GUPTA_EFFICIENCY.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_MAXIMUM_LEFT.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_MAXIMUM_RIGHT.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_MAXIMUM.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_MEAN_ABSOLUTE_ERROR.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_MEAN_ABSOLUTE_ERROR.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_MEAN_ABSOLUTE_ERROR_SKILL_SCORE.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_MEAN_ABSOLUTE_ERROR_SKILL_SCORE.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_MEAN.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_MEAN_LEFT.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_MEAN_RIGHT.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_MEAN_ERROR.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_MEAN_ERROR.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_MEAN_SQUARE_ERROR.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_MEAN_SQUARE_ERROR.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_MEAN_SQUARE_ERROR_SKILL_SCORE.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_MEAN_SQUARE_ERROR_SKILL_SCORE_NORMALIZED.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_MEAN_SQUARE_ERROR_SKILL_SCORE_NORMALIZED.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_MEAN_SQUARE_ERROR_SKILL_SCORE.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_MEDIAN_ERROR.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_MEDIAN_ERROR.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_MINIMUM_LEFT.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_MINIMUM_RIGHT.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_MINIMUM.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_PEARSON_CORRELATION_COEFFICIENT.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_PEARSON_CORRELATION_COEFFICIENT.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_PEIRCE_SKILL_SCORE.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_PEIRCE_SKILL_SCORE.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_PROBABILITY_OF_DETECTION.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_PROBABILITY_OF_DETECTION.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_PROBABILITY_OF_FALSE_DETECTION.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_PROBABILITY_OF_FALSE_DETECTION.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_QUANTILE_QUANTILE_DIAGRAM_151200_SECONDS.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_QUANTILE_QUANTILE_DIAGRAM_151200_SECONDS.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_SCATTER_PLOT_151200_SECONDS.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_SCATTER_PLOT_151200_SECONDS.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_RANK_HISTOGRAM_151200_SECONDS.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_RANK_HISTOGRAM_151200_SECONDS.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_151200_SECONDS.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_151200_SECONDS.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_SCORE.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_SCORE.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_RELIABILITY_DIAGRAM_151200_SECONDS.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_RELIABILITY_DIAGRAM_151200_SECONDS.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_ROOT_MEAN_SQUARE_ERROR.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_ROOT_MEAN_SQUARE_ERROR_NORMALIZED.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_ROOT_MEAN_SQUARE_ERROR_NORMALIZED.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_ROOT_MEAN_SQUARE_ERROR.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_SAMPLE_SIZE.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_SAMPLE_SIZE.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_STANDARD_DEVIATION.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_STANDARD_DEVIATION_LEFT.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_STANDARD_DEVIATION_RIGHT.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_SUM_OF_SQUARE_ERROR.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_SUM_OF_SQUARE_ERROR.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_THREAT_SCORE.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_THREAT_SCORE.png" ),
                    Path.of( "LGNN5_LGNN5_HEFS_VOLUMETRIC_EFFICIENCY.csv" ),
                    Path.of( "LGNN5_LGNN5_HEFS_VOLUMETRIC_EFFICIENCY.png" ),
                    Path.of( "pairs.csv.gz" ) );

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

