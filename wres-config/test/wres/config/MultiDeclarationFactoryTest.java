package wres.config;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.Formats;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.MetricBuilder;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceBuilder;
import wres.statistics.generated.Outputs;

/**
 * Tests the {@link MultiDeclarationFactory}.
 *
 * @author James Brown
 */
class MultiDeclarationFactoryTest
{
    /** An observed dataset for re-use. */
    private Dataset observedDataset;
    /** A predicted dataset for re-use. */
    private Dataset predictedDataset;

    @BeforeEach
    void runBeforeEach()
    {
        URI observedUri = URI.create( "some_file.csv" );
        Source observedSource = SourceBuilder.builder()
                                             .uri( observedUri )
                                             .build();

        URI predictedUri = URI.create( "another_file.csv" );
        Source predictedSource = SourceBuilder.builder()
                                              .uri( predictedUri )
                                              .build();

        List<Source> observedSources = List.of( observedSource );
        this.observedDataset = DatasetBuilder.builder()
                                             .type( DataType.OBSERVATIONS )
                                             .sources( observedSources )
                                             .build();

        List<Source> predictedSources = List.of( predictedSource );
        this.predictedDataset = DatasetBuilder.builder()
                                              .type( DataType.ENSEMBLE_FORECASTS )
                                              .sources( predictedSources )
                                              .build();
    }

    @Test
    void testDeserializeFromOldDeclarationLanguageUsingExplicitString() throws IOException
    {
        String oldLanguageString = """
                <?xml version="1.0" encoding="UTF-8"?>
                <project name="scenario50x">
                    <inputs>
                        <left>
                            <type>observations</type>
                            <source>some_file.csv</source>
                        </left>
                        <right>
                            <type>ensemble forecasts</type>
                            <source>another_file.csv</source>
                        </right>
                    </inputs>
                    <pair>
                        <unit>CMS</unit>
                    </pair>
                    <metrics>
                        <metric><name>sample size</name></metric>
                    </metrics>
                    <outputs>
                        <destination type="pairs" />
                    </outputs>
                </project>""";

        EvaluationDeclaration actual = MultiDeclarationFactory.from( oldLanguageString, null, true );

        Metric metric = MetricBuilder.builder()
                                     .name( MetricConstants.SAMPLE_SIZE )
                                     .build();
        Outputs.NumericFormat numericFormat = Outputs.NumericFormat.newBuilder()
                                                                   .setDecimalFormat( "#0.000000" )
                                                                   .build();
        Outputs formatsInner = Outputs.newBuilder()
                                      .setPairs( Outputs.PairFormat.newBuilder()
                                                                   .setOptions( numericFormat )
                                                                   .build() )
                                      .build();
        Formats formats = new Formats( formatsInner );
        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .metrics( Set.of( metric ) )
                                                                     .unit( "CMS" )
                                                                     .formats( formats )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeFromNewDeclarationLanguageUsingExplicitString() throws IOException
    {
        String yaml = """
                observed:
                  sources:
                    - some_file.csv
                  type: observations
                predicted:
                  sources:
                    - another_file.csv
                  type: ensemble forecasts""";

        EvaluationDeclaration actual = MultiDeclarationFactory.from( yaml, null, false );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeFromNewDeclarationLanguageUsingPathToFile() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            Path path = fileSystem.getPath( "foo.file" );
            Files.createFile( path );
            String pathString = path.toString();

            String yaml = """
                    observed:
                      sources:
                        - some_file.csv
                      type: observations
                    predicted:
                      sources:
                        - another_file.csv
                      type: ensemble forecasts
                      """;

            Files.writeString( path, yaml );

            EvaluationDeclaration actual = MultiDeclarationFactory.from( pathString, fileSystem, false );

            EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                         .left( this.observedDataset )
                                                                         .right( this.predictedDataset )
                                                                         .build();
            assertEquals( expected, actual );
        }
    }
}
