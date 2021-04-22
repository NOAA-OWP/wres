package wres.io.project;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProjectScriptGeneratorTest
{
    @Test
    void verifyAlphaNumericFeatureWithSpacesWorks()
    {
        String featureName = "ABC 123";
        String result =
                ProjectScriptGenerator.validateStringForSql( featureName );
        Assertions.assertEquals( result, featureName );
    }

    @Test
    void verifyFeatureWithSemicolonFails()
    {
        String featureName = "AAA;AAA";
        Assertions.assertThrows( IllegalArgumentException.class,
                                 () -> ProjectScriptGenerator.validateStringForSql(
                                         featureName ) );
    }

    @Test
    void verifyFeatureWithSingleQuoteFails()
    {
        String featureName = "AAA'AAA";
        Assertions.assertThrows( IllegalArgumentException.class,
                                 () -> ProjectScriptGenerator.validateStringForSql(
                                         featureName ) );
    }

    @Test
    void verifyFeatureWithUnderscoreWorks()
    {
        String featureName = "AAA_AAA";
        String result =
                ProjectScriptGenerator.validateStringForSql( featureName );
        Assertions.assertEquals( result, featureName );
    }

    @Test
    void verifyFeatureWithHyphenWorks()
    {
        String featureName = "AAA-AAA";
        String result =
                ProjectScriptGenerator.validateStringForSql( featureName );
        Assertions.assertEquals( result, featureName );
    }
}
