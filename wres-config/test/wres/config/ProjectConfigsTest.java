package wres.config;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProbabilityThresholdConfig;
import wres.config.generated.ThresholdOperator;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class ProjectConfigsTest
{
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testSaneProbabilityList() throws ProjectConfigException
    {
        String probabilities = "0.2, 0.5,0.8";

        LeftOrRightOrBaseline applyToLeft = LeftOrRightOrBaseline.fromValue( "left" );
        ThresholdOperator operator = ThresholdOperator.fromValue( "less_than" );
        ProbabilityThresholdConfig config =
                new ProbabilityThresholdConfig( applyToLeft,
                                                probabilities,
                                                operator );

        List<Double> values = ProjectConfigs.parseProbabilities( config );

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
        LeftOrRightOrBaseline applyToLeft = LeftOrRightOrBaseline.fromValue( "left" );
        ThresholdOperator operator = ThresholdOperator.fromValue( "less_than" );

        ProbabilityThresholdConfig config =
                new ProbabilityThresholdConfig( applyToLeft,
                        probabilities,
                        operator );

        exception.expect( ProjectConfigException.class );
        ProjectConfigs.parseProbabilities( config );
    }
}
