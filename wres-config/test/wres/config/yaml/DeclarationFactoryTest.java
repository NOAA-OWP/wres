package wres.config.yaml;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests the {@link DeclarationFactory}.
 *
 * @author James Brown
 */
class DeclarationFactoryTest
{
    @Test
    void testDeserializeWithSimplestLeftAndRightDeclaration() throws IOException
    {
        String yaml = """
                left:
                  - some_file.csv
                right:
                  - forecasts_with_NWS_feature_authority.csv""";

        DeclarationFactory.EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        DeclarationFactory.Source leftSource = new DeclarationFactory.Source( URI.create( "some_file.csv" ),
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              null );

        DeclarationFactory.Source rightSource =
                new DeclarationFactory.Source( URI.create( "forecasts_with_NWS_feature_authority.csv" ),
                                               null,
                                               null,
                                               null,
                                               null,
                                               null );

        DeclarationFactory.Dataset leftDataset = new DeclarationFactory.Dataset( List.of( leftSource ),
                                                                                 null,
                                                                                 null,
                                                                                 null,
                                                                                 null,
                                                                                 null,
                                                                                 null );

        DeclarationFactory.Dataset rightDataset = new DeclarationFactory.Dataset( List.of( rightSource ),
                                                                                  null,
                                                                                  null,
                                                                                  null,
                                                                                  null,
                                                                                  null,
                                                                                  null );

        DeclarationFactory.EvaluationDeclaration expected = new DeclarationFactory.EvaluationDeclaration( leftDataset,
                                                                                                          rightDataset,
                                                                                                          null,
                                                                                                          null,
                                                                                                          null,
                                                                                                          null,
                                                                                                          null,
                                                                                                          null,
                                                                                                          null,
                                                                                                          null,
                                                                                                          null,
                                                                                                          null,
                                                                                                          null,
                                                                                                          null,
                                                                                                          null );

        assertEquals( expected, actual );
    }

}
