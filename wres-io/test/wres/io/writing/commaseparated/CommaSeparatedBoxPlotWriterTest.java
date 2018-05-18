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
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.MetricOutputAccessException;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByMetric;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;

/**
 * Tests the writing of box plot outputs to a file of Comma Separated Values (CSV).
 */

public class CommaSeparatedBoxPlotWriterTest extends CommaSeparatedWriterTest
{

    /**
     * Tests the writing of {@link BoxPlotOutput} to file.
     * 
     * @throws ProjectConfigException if the project configuration is incorrect
     * @throws IOException if the output could not be written
     * @throws InterruptedException if the process is interrupted
     * @throws ExecutionException if the execution fails
     * @throws MetricOutputAccessException if the metric output could not be accessed
     */

    @Test
    public void writeBoxPlotOutput()
            throws IOException, InterruptedException,
            ExecutionException, MetricOutputAccessException
    {

        // location id
        final String LID = "JUNP1";

        // Create fake outputs

        DataFactory outputFactory = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = outputFactory.getMetadataFactory();

        MetricOutputForProjectByTimeAndThreshold.MetricOutputForProjectByTimeAndThresholdBuilder outputBuilder =
                outputFactory.ofMetricOutputForProjectByTimeAndThreshold();

        TimeWindow timeOne =
                TimeWindow.of( Instant.MIN,
                               Instant.MAX,
                               ReferenceTime.VALID_TIME,
                               Duration.ofHours( 24 ),
                               Duration.ofHours( 24 ) );

        // Output requires a future... which requires a metadata...
        // which requires a datasetidentifier..

        DatasetIdentifier datasetIdentifier =
                metaFac.getDatasetIdentifier( LID,
                                              "SQIN",
                                              "HEFS",
                                              "ESP" );

        MetricOutputMetadata fakeMetadata =
                metaFac.getOutputMetadata( 1000,
                                           metaFac.getDimension(),
                                           metaFac.getDimension( "CMS" ),
                                           MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                           null,
                                           datasetIdentifier );

        List<PairOfDoubleAndVectorOfDoubles> fakeOutputs = new ArrayList<>();
        VectorOfDoubles probs = outputFactory.vectorOf( new double[] { 0, 0.25, 0.5, 0.75, 1.0 } );

        fakeOutputs.add( outputFactory.pairOf( 1, new double[] { 2, 3, 4, 5, 6 } ) );
        fakeOutputs.add( outputFactory.pairOf( 3, new double[] { 7, 9, 11, 13, 15 } ) );
        fakeOutputs.add( outputFactory.pairOf( 5, new double[] { 21, 24, 27, 30, 33 } ) );

        // Fake output wrapper.
        MetricOutputMapByMetric<BoxPlotOutput> fakeOutputData =
                outputFactory.ofMetricOutputMapByMetric( Collections.singletonMap( MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                                                                   outputFactory.ofBoxPlotOutput( fakeOutputs,
                                                                                                                  probs,
                                                                                                                  fakeMetadata,
                                                                                                                  MetricDimension.OBSERVED_VALUE,
                                                                                                                  MetricDimension.FORECAST_ERROR ) ) );
        // wrap outputs in future
        Future<MetricOutputMapByMetric<BoxPlotOutput>> outputMapByMetricFuture =
                CompletableFuture.completedFuture( fakeOutputData );


        // Fake lead time and threshold
        Pair<TimeWindow, OneOrTwoThresholds> mapKeyByLeadThreshold =
                Pair.of( timeOne,
                         OneOrTwoThresholds.of( outputFactory.ofThreshold( outputFactory.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
                                                                           Operator.GREATER,
                                                                           ThresholdDataType.LEFT ) ) );

        outputBuilder.addBoxPlotOutput( mapKeyByLeadThreshold,
                                        outputMapByMetricFuture );

        MetricOutputForProjectByTimeAndThreshold output = outputBuilder.build();

        // Construct a fake configuration file.
        Feature feature = getMockedFeature( LID );
        ProjectConfig projectConfig = getMockedProjectConfig( feature );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparatedBoxPlotWriter.of( projectConfig ).accept( output.getBoxPlotOutput() );

        // read the file, verify it has what we wanted:
        Path pathToFile = Paths.get( System.getProperty( "java.io.tmpdir" ),
                                     "JUNP1_SQIN_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_24_HOUR.csv" );
        List<String> result = Files.readAllLines( pathToFile );

        assertTrue( result.get( 0 ).contains( "," ) );
        assertTrue( result.get( 0 ).contains( "OBSERVED VALUE" ) );
        assertTrue( result.get( 0 ).contains( "FORECAST ERROR" ) );

        assertTrue( result.get( 1 )
                          .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,24,24,"
                                   + "1.0,2.0,3.0,4.0,5.0,6.0" ) );
        assertTrue( result.get( 2 )
                          .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,24,24,"
                                   + "3.0,7.0,9.0,11.0,13.0,15.0" ) );
        assertTrue( result.get( 3 )
                          .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,24,24,"
                                   + "5.0,21.0,24.0,27.0,30.0,33.0" ) );
        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFile );
    }

}
