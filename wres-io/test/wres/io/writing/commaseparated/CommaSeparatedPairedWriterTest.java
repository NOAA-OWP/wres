package wres.io.writing.commaseparated;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import wres.config.ProjectConfigException;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.MetricOutputAccessException;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByMetric;
import wres.datamodel.outputs.PairedOutput;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;

/**
 * Tests the writing of paired outputs to a file of Comma Separated Values (CSV).
 */

public class CommaSeparatedPairedWriterTest extends CommaSeparatedWriterTest
{

    /**
     * Tests the writing of {@link PairedOutput} to file, where the left pair comprises an {@link Instant} and the
     * right pair comprises an (@link Duration).
     * 
     * @throws ProjectConfigException if the project configuration is incorrect
     * @throws IOException if the output could not be written
     * @throws InterruptedException if the process is interrupted
     * @throws ExecutionException if the execution fails
     * @throws MetricOutputAccessException if the metric output could not be accessed
     */

    @Test
    public void writePairedOutputForTimeSeriesMetrics()
            throws ProjectConfigException, IOException, InterruptedException,
            ExecutionException, MetricOutputAccessException
    {

        // location id
        final String LID = "FTSC1";

        // Create fake outputs

        DataFactory outputFactory = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = outputFactory.getMetadataFactory();

        MetricOutputForProjectByTimeAndThreshold.MetricOutputForProjectByTimeAndThresholdBuilder outputBuilder =
                outputFactory.ofMetricOutputForProjectByTimeAndThreshold();

        TimeWindow timeOne =
                TimeWindow.of( Instant.MIN,
                               Instant.MAX,
                               ReferenceTime.VALID_TIME,
                               Duration.ofHours( 1 ),
                               Duration.ofHours( 18 ) );

        // Output requires a future... which requires a metadata...
        // which requires a datasetidentifier..

        DatasetIdentifier datasetIdentifier =
                metaFac.getDatasetIdentifier( metaFac.getLocation( LID ),
                                              "SQIN",
                                              "HEFS",
                                              "ESP" );

        MetricOutputMetadata fakeMetadata =
                metaFac.getOutputMetadata( 1000,
                                           metaFac.getDimension(),
                                           metaFac.getDimension( "CMS" ),
                                           MetricConstants.TIME_TO_PEAK_ERROR,
                                           null,
                                           datasetIdentifier );

        List<Pair<Instant, Duration>> fakeOutputs = new ArrayList<>();
        fakeOutputs.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( 1 ) ) );
        fakeOutputs.add( Pair.of( Instant.parse( "1985-01-02T00:00:00Z" ), Duration.ofHours( 2 ) ) );
        fakeOutputs.add( Pair.of( Instant.parse( "1985-01-03T00:00:00Z" ), Duration.ofHours( 3 ) ) );

        // Fake output wrapper.
        MetricOutputMapByMetric<PairedOutput<Instant, Duration>> fakeOutputData =
                outputFactory.ofMetricOutputMapByMetric( Collections.singletonMap( MetricConstants.TIME_TO_PEAK_ERROR,
                                                                                   outputFactory.ofPairedOutput( fakeOutputs,
                                                                                                                 fakeMetadata ) ) );

        // wrap outputs in future
        Future<MetricOutputMapByMetric<PairedOutput<Instant, Duration>>> outputMapByMetricFuture =
                CompletableFuture.completedFuture( fakeOutputData );

        // Fake lead time and threshold
        Pair<TimeWindow, OneOrTwoThresholds> mapKeyByLeadThreshold =
                outputFactory.ofMapKeyByTimeThreshold( timeOne,
                                                       outputFactory.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
                                                       Operator.GREATER,
                                                       ThresholdDataType.LEFT );

        outputBuilder.addPairedOutput( mapKeyByLeadThreshold,
                                       outputMapByMetricFuture );

        MetricOutputForProjectByTimeAndThreshold output = outputBuilder.build();

        // Construct a fake configuration file.
        Feature feature = getMockedFeature( LID );
        ProjectConfig projectConfig = getMockedProjectConfig( feature );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparatedPairedWriter<Instant, Duration> writer = CommaSeparatedPairedWriter.of( projectConfig );
        writer.accept( output.getPairedOutput() );

        // read the file, verify it has what we wanted:
        Path pathToFile = Paths.get( System.getProperty( "java.io.tmpdir" ),
                                     "FTSC1_SQIN_HEFS_TIME_TO_PEAK_ERROR.csv" );
        List<String> result = Files.readAllLines( pathToFile );

        assertTrue( result.get( 0 ).contains( "," ) );
        assertTrue( result.get( 0 ).contains( "ERROR" ) );
        assertTrue( result.get( 1 )
                          .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,1,18,"
                                   + "1985-01-01T00:00:00Z,PT1H" ) );
        assertTrue( result.get( 2 )
                          .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,1,18,"
                                   + "1985-01-02T00:00:00Z,PT2H" ) );
        assertTrue( result.get( 3 )
                          .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,1,18,"
                                   + "1985-01-03T00:00:00Z,PT3H" ) );
        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFile );
    }

}
