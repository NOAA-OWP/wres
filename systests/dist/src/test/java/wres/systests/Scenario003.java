package wres.systests;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.control.Control;
public class Scenario003
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Scenario003.class );
    private static final String NEWLINE = System.lineSeparator();

    private ScenarioInformation scenarioInfo;

    /**
     * Expected paths as file names.
     */

    private static final Set<Path> EXPECTED_FILE_NAMES =
            Set.of( Path.of( "DRRC2_QINE_HEFS_BIAS_FRACTION.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BIAS_FRACTION.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_10800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_10800_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_14400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_14400_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_18000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_18000_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_21600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_21600_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_25200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_25200_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_28800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_28800_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_32400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_32400_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_36000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_36000_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_3600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_3600_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_39600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_39600_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_43200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_43200_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_46800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_46800_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_50400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_50400_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_54000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_54000_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_57600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_57600_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_61200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_61200_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_64800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_64800_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_68400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_68400_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_72000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_72000_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_7200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_7200_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_75600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_75600_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_79200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_79200_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_82800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_82800_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_86400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE_86400_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_10800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_10800_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_14400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_14400_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_18000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_18000_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_21600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_21600_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_25200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_25200_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_28800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_28800_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_32400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_32400_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_36000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_36000_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_3600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_3600_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_39600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_39600_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_43200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_43200_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_46800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_46800_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_50400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_50400_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_54000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_54000_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_57600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_57600_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_61200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_61200_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_64800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_64800_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_68400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_68400_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_72000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_72000_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_7200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_7200_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_75600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_75600_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_79200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_79200_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_82800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_82800_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_86400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_86400_SECONDS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_ERRORS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_PERCENTAGE_ERRORS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BOX_PLOT_OF_PERCENTAGE_ERRORS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BRIER_SCORE.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BRIER_SCORE.png" ),
                    Path.of( "DRRC2_QINE_HEFS_BRIER_SKILL_SCORE.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_BRIER_SKILL_SCORE.png" ),
                    Path.of( "DRRC2_QINE_HEFS_COEFFICIENT_OF_DETERMINATION.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_COEFFICIENT_OF_DETERMINATION.png" ),
                    Path.of( "DRRC2_QINE_HEFS_CONTINUOUS_RANKED_PROBABILITY_SCORE.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_CONTINUOUS_RANKED_PROBABILITY_SCORE.png" ),
                    Path.of( "DRRC2_QINE_HEFS_INDEX_OF_AGREEMENT.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_INDEX_OF_AGREEMENT.png" ),
                    Path.of( "DRRC2_QINE_HEFS_KLING_GUPTA_EFFICIENCY.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_KLING_GUPTA_EFFICIENCY.png" ),
                    Path.of( "DRRC2_QINE_HEFS_MEAN_ABSOLUTE_ERROR.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_MEAN_ABSOLUTE_ERROR.png" ),
                    Path.of( "DRRC2_QINE_HEFS_MEAN_ERROR.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_MEAN_ERROR.png" ),
                    Path.of( "DRRC2_QINE_HEFS_MEAN_SQUARE_ERROR.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_MEAN_SQUARE_ERROR.png" ),
                    Path.of( "DRRC2_QINE_HEFS_MEAN_SQUARE_ERROR_SKILL_SCORE.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_MEAN_SQUARE_ERROR_SKILL_SCORE_NORMALIZED.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_MEAN_SQUARE_ERROR_SKILL_SCORE_NORMALIZED.png" ),
                    Path.of( "DRRC2_QINE_HEFS_MEAN_SQUARE_ERROR_SKILL_SCORE.png" ),
                    Path.of( "DRRC2_QINE_HEFS_MEDIAN_ERROR.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_MEDIAN_ERROR.png" ),
                    Path.of( "DRRC2_QINE_HEFS_PEARSON_CORRELATION_COEFFICIENT.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_PEARSON_CORRELATION_COEFFICIENT.png" ),
                    Path.of( "DRRC2_QINE_HEFS_QUANTILE_QUANTILE_DIAGRAM_10800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_QUANTILE_QUANTILE_DIAGRAM_14400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_QUANTILE_QUANTILE_DIAGRAM_18000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_QUANTILE_QUANTILE_DIAGRAM_21600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_QUANTILE_QUANTILE_DIAGRAM_25200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_QUANTILE_QUANTILE_DIAGRAM_28800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_QUANTILE_QUANTILE_DIAGRAM_32400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_QUANTILE_QUANTILE_DIAGRAM_36000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_QUANTILE_QUANTILE_DIAGRAM_3600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_QUANTILE_QUANTILE_DIAGRAM_39600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_QUANTILE_QUANTILE_DIAGRAM_43200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_QUANTILE_QUANTILE_DIAGRAM_46800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_QUANTILE_QUANTILE_DIAGRAM_50400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_QUANTILE_QUANTILE_DIAGRAM_54000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_QUANTILE_QUANTILE_DIAGRAM_57600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_QUANTILE_QUANTILE_DIAGRAM_61200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_QUANTILE_QUANTILE_DIAGRAM_64800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_QUANTILE_QUANTILE_DIAGRAM_68400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_QUANTILE_QUANTILE_DIAGRAM_72000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_QUANTILE_QUANTILE_DIAGRAM_7200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_QUANTILE_QUANTILE_DIAGRAM_75600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_QUANTILE_QUANTILE_DIAGRAM_79200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_QUANTILE_QUANTILE_DIAGRAM_82800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_QUANTILE_QUANTILE_DIAGRAM_86400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_QUANTILE_QUANTILE_DIAGRAM_All_data.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_10800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_14400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_18000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_21600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_25200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_28800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_32400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_36000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_3600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_39600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_43200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_46800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_50400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_54000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_57600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_61200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_64800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_68400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_72000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_7200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_75600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_79200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_82800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_86400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_All_data.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_GTE_0.5_CMS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_GTE_10.0_CMS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_GTE_1.0_CMS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_GTE_15.0_CMS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_GTE_20.0_CMS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_GTE_2.0_CMS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_GTE_25.0_CMS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RANK_HISTOGRAM_GTE_5.0_CMS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_10800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_14400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_18000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_21600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_25200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_28800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_32400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_36000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_3600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_39600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_43200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_46800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_50400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_54000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_57600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_61200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_64800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_68400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_72000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_7200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_75600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_79200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_82800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_86400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_GTE_0.5_CMS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_GTE_10.0_CMS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_GTE_1.0_CMS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_GTE_15.0_CMS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_GTE_20.0_CMS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_GTE_2.0_CMS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_GTE_25.0_CMS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM_GTE_5.0_CMS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_SCORE.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELATIVE_OPERATING_CHARACTERISTIC_SCORE.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_10800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_14400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_18000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_21600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_25200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_28800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_32400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_36000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_3600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_39600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_43200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_46800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_50400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_54000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_57600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_61200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_64800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_68400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_72000_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_7200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_75600_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_79200_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_82800_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_86400_SECONDS.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_GTE_0.5_CMS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_GTE_10.0_CMS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_GTE_1.0_CMS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_GTE_15.0_CMS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_GTE_20.0_CMS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_GTE_2.0_CMS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_GTE_25.0_CMS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_RELIABILITY_DIAGRAM_GTE_5.0_CMS.png" ),
                    Path.of( "DRRC2_QINE_HEFS_ROOT_MEAN_SQUARE_ERROR.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_ROOT_MEAN_SQUARE_ERROR_NORMALIZED.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_ROOT_MEAN_SQUARE_ERROR_NORMALIZED.png" ),
                    Path.of( "DRRC2_QINE_HEFS_ROOT_MEAN_SQUARE_ERROR.png" ),
                    Path.of( "DRRC2_QINE_HEFS_SAMPLE_SIZE.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_SAMPLE_SIZE.png" ),
                    Path.of( "DRRC2_QINE_HEFS_SUM_OF_SQUARE_ERROR.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_SUM_OF_SQUARE_ERROR.png" ),
                    Path.of( "DRRC2_QINE_HEFS_VOLUMETRIC_EFFICIENCY.csv" ),
                    Path.of( "DRRC2_QINE_HEFS_VOLUMETRIC_EFFICIENCY.png" ),
                    Path.of( "pairs.csv" ) );
    
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

        // Expected file-name paths equals actual
        LOGGER.info( "Checking expected file names against actual file names that exist for {} files...",
                     EXPECTED_FILE_NAMES.size() );

        assertEquals( "The actual set of file names does not match the expected set of file names.",
                      EXPECTED_FILE_NAMES,
                      actualFileNamesThatExist );
        
        LOGGER.info( "Finished checking file names. The actual file names match the expected file names." );        
        
        ScenarioHelper.assertOutputsMatchBenchmarks( scenarioInfo, control );
        LOGGER.info( "########################################################## COMPLETED "
                + this.getClass().getSimpleName().toLowerCase() + NEWLINE);
    }
}
