package wres.config;

import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdsConfig;
import wres.datamodel.thresholds.ThresholdConstants.Operator;

public class ProjectConfigsTest
{
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Tests the {@link ProjectConfigs#getThresholdOperator(ThresholdsConfig)}.
     * @throws MetricConfigException if a mapping could not be created
     */

    @Test
    public void testGetThresholdOperator() throws MetricConfigException
    {
        ThresholdsConfig first = new ThresholdsConfig( null,
                                                       null,
                                                       null,
                                                       ThresholdOperator.GREATER_THAN );
        assertTrue( "Failed to convert '" + ThresholdOperator.GREATER_THAN
                    + "'.",
                    ProjectConfigs.getThresholdOperator( first ) == Operator.GREATER );

        ThresholdsConfig second = new ThresholdsConfig( null,
                                                        null,
                                                        null,
                                                        ThresholdOperator.LESS_THAN );
        assertTrue( "Failed to convert '" + ThresholdOperator.LESS_THAN
                    + "'.",
                    ProjectConfigs.getThresholdOperator( second ) == Operator.LESS );

        ThresholdsConfig third = new ThresholdsConfig( null,
                                                       null,
                                                       null,
                                                       ThresholdOperator.GREATER_THAN_OR_EQUAL_TO );
        assertTrue( "Failed to convert '" + ThresholdOperator.GREATER_THAN_OR_EQUAL_TO
                    + "'.",
                    ProjectConfigs.getThresholdOperator( third ) == Operator.GREATER_EQUAL );

        ThresholdsConfig fourth = new ThresholdsConfig( null,
                                                        null,
                                                        null,
                                                        ThresholdOperator.LESS_THAN_OR_EQUAL_TO );
        assertTrue( "Failed to convert '" + ThresholdOperator.LESS_THAN_OR_EQUAL_TO
                    + "'.",
                    ProjectConfigs.getThresholdOperator( fourth ) == Operator.LESS_EQUAL );

        //Test exception cases
        exception.expect( NullPointerException.class );
        ProjectConfigs.getThresholdOperator( (ThresholdsConfig) null );

        ProjectConfigs.getThresholdOperator( new ThresholdsConfig( null,
                                                                   null,
                                                                   null,
                                                                   null ) );
    }

}
