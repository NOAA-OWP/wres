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

public class Scenario400
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Scenario400.class );
    private static final String NEWLINE = System.lineSeparator();

    /**
     * Expected paths as file names.
     */

    private static final Set<Path> EXPECTED_FILE_NAMES =
            Set.of( Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_BIAS_FRACTION.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_BIAS_FRACTION.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_BOX_PLOT_OF_ERRORS.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_BOX_PLOT_OF_ERRORS.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_BOX_PLOT_OF_PERCENTAGE_ERRORS.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_BOX_PLOT_OF_PERCENTAGE_ERRORS.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_COEFFICIENT_OF_DETERMINATION.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_COEFFICIENT_OF_DETERMINATION.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_CONTINGENCY_TABLE.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_EQUITABLE_THREAT_SCORE.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_EQUITABLE_THREAT_SCORE.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_FREQUENCY_BIAS.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_FREQUENCY_BIAS.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_FALSE_ALARM_RATIO.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_FALSE_ALARM_RATIO.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_INDEX_OF_AGREEMENT.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_INDEX_OF_AGREEMENT.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_KLING_GUPTA_EFFICIENCY.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_KLING_GUPTA_EFFICIENCY.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_MAXIMUM.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_MAXIMUM_LEFT.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_MAXIMUM_RIGHT.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_MEAN_ABSOLUTE_ERROR.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_MEAN_ABSOLUTE_ERROR.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_MEAN_ABSOLUTE_ERROR_SKILL_SCORE.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_MEAN_ABSOLUTE_ERROR_SKILL_SCORE.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_MEAN.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_MEAN_LEFT.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_MEAN_RIGHT.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_MEAN_ERROR.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_MEAN_ERROR.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_MEAN_SQUARE_ERROR.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_MEAN_SQUARE_ERROR.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_MEAN_SQUARE_ERROR_SKILL_SCORE.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_MEAN_SQUARE_ERROR_SKILL_SCORE_NORMALIZED.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_MEAN_SQUARE_ERROR_SKILL_SCORE_NORMALIZED.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_MEAN_SQUARE_ERROR_SKILL_SCORE.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_MEDIAN_ERROR.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_MEDIAN_ERROR.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_MINIMUM.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_MINIMUM_LEFT.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_MINIMUM_RIGHT.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_PEARSON_CORRELATION_COEFFICIENT.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_PEARSON_CORRELATION_COEFFICIENT.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_PEIRCE_SKILL_SCORE.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_PEIRCE_SKILL_SCORE.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_PROBABILITY_OF_DETECTION.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_PROBABILITY_OF_DETECTION.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_PROBABILITY_OF_FALSE_DETECTION.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_PROBABILITY_OF_FALSE_DETECTION.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_QUANTILE_QUANTILE_DIAGRAM_21600_SECONDS.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_QUANTILE_QUANTILE_DIAGRAM_21600_SECONDS.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_QUANTILE_QUANTILE_DIAGRAM_43200_SECONDS.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_QUANTILE_QUANTILE_DIAGRAM_43200_SECONDS.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_QUANTILE_QUANTILE_DIAGRAM_64800_SECONDS.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_QUANTILE_QUANTILE_DIAGRAM_64800_SECONDS.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_QUANTILE_QUANTILE_DIAGRAM_86400_SECONDS.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_QUANTILE_QUANTILE_DIAGRAM_86400_SECONDS.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_ROOT_MEAN_SQUARE_ERROR.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_ROOT_MEAN_SQUARE_ERROR_NORMALIZED.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_ROOT_MEAN_SQUARE_ERROR_NORMALIZED.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_ROOT_MEAN_SQUARE_ERROR.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_SAMPLE_SIZE.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_SAMPLE_SIZE.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_STANDARD_DEVIATION.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_STANDARD_DEVIATION_LEFT.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_STANDARD_DEVIATION_RIGHT.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_SUM_OF_SQUARE_ERROR.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_SUM_OF_SQUARE_ERROR.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_THREAT_SCORE.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_THREAT_SCORE.png" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_VOLUMETRIC_EFFICIENCY.csv" ),
                    Path.of( "GLOO2X_GLOO2_Operational_Single-Valued_Forecasts_VOLUMETRIC_EFFICIENCY.png" ),
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
                     + this.getClass().getSimpleName().toLowerCase()
                     + NEWLINE );
    }
}

