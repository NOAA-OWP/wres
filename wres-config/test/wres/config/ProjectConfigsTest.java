package wres.config;

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProbabilityOrValue;
import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdsConfig;

public class ProjectConfigsTest
{
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testSaneProbabilityList() throws ProjectConfigException
    {
        String probabilities = "0.2, 0.5,0.8";

        ProbabilityOrValue probability = ProbabilityOrValue.fromValue( "probability" );
        LeftOrRightOrBaseline applyToLeft = LeftOrRightOrBaseline.fromValue( "left" );
        ThresholdOperator operator = ThresholdOperator.fromValue( "less than" );
        ThresholdsConfig config = new ThresholdsConfig( probability,
                                                        applyToLeft,
                                                        probabilities,
                                                        operator );

        List<Double> values = ProjectConfigs.parseValues( config );

        assertTrue( "Expected 0.2 to be in first position.",
                    0 == Double.compare( 0.2, values.get( 0 ) ) );
        assertTrue( "Expected 0.5 to be in second position.",
                0 == Double.compare( 0.5, values.get( 1 ) ) );
        assertTrue( "Expected 0.8 to be in first position.",
                0 == Double.compare( 0.8, values.get( 2 ) ) );

    }

    @Test
    public void testProblematicProbabilityList() throws ProjectConfigException
    {
        String probabilities = "boogaflickle";
        ProbabilityOrValue probability = ProbabilityOrValue.fromValue( "probability" );
        LeftOrRightOrBaseline applyToLeft = LeftOrRightOrBaseline.fromValue( "left" );
        ThresholdOperator operator = ThresholdOperator.fromValue( "less than" );

        ThresholdsConfig config = new ThresholdsConfig( probability,
                                                        applyToLeft,
                                                        probabilities,
                                                        operator );

        exception.expect( ProjectConfigException.class );
        ProjectConfigs.parseValues( config );
    }

    @Test
    public void testInvalidNegativeValueProbabilityList() throws ProjectConfigException
    {
        String probabilities = "-5.0";
        ProbabilityOrValue probability = ProbabilityOrValue.fromValue( "probability" );
        LeftOrRightOrBaseline applyToLeft = LeftOrRightOrBaseline.fromValue( "left" );
        ThresholdOperator operator = ThresholdOperator.fromValue( "less than" );

        ThresholdsConfig config = new ThresholdsConfig( probability,
                                                        applyToLeft,
                                                        probabilities,
                                                        operator );

        exception.expect( ProjectConfigException.class );
        ProjectConfigs.parseValues( config );
    }

    @Test
    public void testInvalidPositiveValueProbabilityList() throws ProjectConfigException
    {
        String probabilities = "1.2";
        ProbabilityOrValue probability = ProbabilityOrValue.fromValue( "probability" );
        LeftOrRightOrBaseline applyToLeft = LeftOrRightOrBaseline.fromValue( "left" );
        ThresholdOperator operator = ThresholdOperator.fromValue( "less than" );

        ThresholdsConfig config = new ThresholdsConfig( probability,
                                                        applyToLeft,
                                                        probabilities,
                                                        operator );

        exception.expect( ProjectConfigException.class );
        ProjectConfigs.parseValues( config );
    }

    @Test
    public void testSaneValueList() throws ProjectConfigException
    {
        String values = "200, 50.0,-8";
        ProbabilityOrValue probability = ProbabilityOrValue.fromValue( "value" );
        LeftOrRightOrBaseline applyToLeft = LeftOrRightOrBaseline.fromValue( "left" );
        ThresholdOperator operator = ThresholdOperator.fromValue( "less than" );
        ThresholdsConfig config =
                new ThresholdsConfig( probability, applyToLeft, values, operator );

        List<Double> results = ProjectConfigs.parseValues( config );

        assertTrue( "Expected 0.2 to be in first position.",
                0 == Double.compare( 200, results.get( 0 ) ) );
        assertTrue( "Expected 0.5 to be in second position.",
                0 == Double.compare( 50, results.get( 1 ) ) );
        assertTrue( "Expected 0.8 to be in first position.",
                0 == Double.compare( -8, results.get( 2 ) ) );
    }

    @Test
    public void testProblematicValueList() throws ProjectConfigException
    {
        String values = "schnitzelbank";
        ProbabilityOrValue probability = ProbabilityOrValue.fromValue( "value" );
        LeftOrRightOrBaseline applyToLeft = LeftOrRightOrBaseline.fromValue( "left" );
        ThresholdOperator operator = ThresholdOperator.fromValue( "less than" );
        ThresholdsConfig config =
                new ThresholdsConfig( probability, applyToLeft, values, operator );

        exception.expect( ProjectConfigException.class );
        ProjectConfigs.parseValues( config );
    }
}
