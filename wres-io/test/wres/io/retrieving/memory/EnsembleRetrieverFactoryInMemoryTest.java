package wres.io.retrieving.memory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.config.components.BaselineDataset;
import wres.config.components.BaselineDatasetBuilder;
import wres.config.components.CovariateDataset;
import wres.config.components.CovariateDatasetBuilder;
import wres.config.components.DataType;
import wres.config.components.Dataset;
import wres.config.components.DatasetBuilder;
import wres.config.components.DatasetOrientation;
import wres.config.components.EvaluationDeclaration;
import wres.config.components.EvaluationDeclarationBuilder;
import wres.config.components.VariableBuilder;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeSeriesStore;
import wres.datamodel.time.TimeWindowOuter;
import wres.datamodel.types.Ensemble;
import wres.io.project.Project;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.ReferenceTime;
import wres.statistics.generated.TimeWindow;

/**
 * Tests the {@link EnsembleRetrieverFactoryInMemory}.
 * @author James Brown
 */
class EnsembleRetrieverFactoryInMemoryTest
{
    /** Variable name. */
    private static final String VARIABLE_NAME = "Q";

    /** Expected left single-valued series.*/
    private TimeSeries<Double> left;

    /** Expected right ensemble series. */
    private TimeSeries<Ensemble> right;

    /** Expected baseline ensemble series. */
    private TimeSeries<Ensemble> baseline;

    /** Expected covariate single-valued series. */
    private TimeSeries<Double> covariate;

    /** Features. */
    private Set<Feature> features;

    /** Test instance. */
    private EnsembleRetrieverFactoryInMemory tester;

    @BeforeEach
    void runBeforeEachTest()
    {
        TimeSeriesStore.Builder builder = new TimeSeriesStore.Builder();


        Feature feature = Feature.of( MessageUtilities.getGeometry( "feature" ) );
        this.features = Set.of( feature );

        TimeSeriesMetadata leftMetadata = TimeSeriesMetadata.of( Map.of(),
                                                                 TimeScaleOuter.of(),
                                                                 "left",
                                                                 feature,
                                                                 "left_unit" );
        this.left = new TimeSeries.Builder<Double>()
                .addEvent( Event.of( Instant.parse( "2123-12-01T06:00:00Z" ), 1.0 ) )
                .setMetadata( leftMetadata )
                .build();

        builder.addSingleValuedSeries( this.left, DatasetOrientation.LEFT );

        TimeSeriesMetadata rightMetadata = TimeSeriesMetadata.of( Map.of( ReferenceTime.ReferenceTimeType.T0,
                                                                          Instant.parse( "2123-12-01T06:00:00Z" ) ),
                                                                  TimeScaleOuter.of(),
                                                                  "right",
                                                                  feature,
                                                                  "right_unit" );

        this.right = new TimeSeries.Builder<Ensemble>()
                .addEvent( Event.of( Instant.parse( "2123-12-01T07:00:00Z" ),
                                     Ensemble.of( 2.0 ) ) )
                .setMetadata( rightMetadata )
                .build();

        builder.addEnsembleSeries( this.right, DatasetOrientation.RIGHT );

        TimeSeriesMetadata baselineMetadata = TimeSeriesMetadata.of( Map.of( ReferenceTime.ReferenceTimeType.T0,
                                                                             Instant.parse( "2123-12-01T06:00:00Z" ) ),
                                                                     TimeScaleOuter.of(),
                                                                     "baseline",
                                                                     feature,
                                                                     "baseline_unit" );

        this.baseline = new TimeSeries.Builder<Ensemble>()
                .addEvent( Event.of( Instant.parse( "2123-12-01T07:00:00Z" ),
                                     Ensemble.of( 3.0 ) ) )
                .setMetadata( baselineMetadata )
                .build();

        builder.addEnsembleSeries( this.baseline, DatasetOrientation.BASELINE );


        TimeSeriesMetadata covariateMetadata = TimeSeriesMetadata.of( Map.of(),
                                                                      TimeScaleOuter.of(),
                                                                      VARIABLE_NAME,
                                                                      feature,
                                                                      "covariate_unit" );

        this.covariate = new TimeSeries.Builder<Double>()
                .addEvent( Event.of( Instant.parse( "2123-12-01T07:00:00Z" ), 4.0 ) )
                .setMetadata( covariateMetadata )
                .build();

        builder.addSingleValuedSeries( this.covariate, DatasetOrientation.COVARIATE );

        TimeSeriesStore store = builder.build();

        Project project = Mockito.mock( Project.class );

        Dataset left = DatasetBuilder.builder()
                                     .type( DataType.OBSERVATIONS )
                                     .variable( VariableBuilder.builder()
                                                               .name( "left" )
                                                               .build() )
                                     .build();

        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.SINGLE_VALUED_FORECASTS )
                                      .variable( VariableBuilder.builder()
                                                                .name( "right" )
                                                                .build() )
                                      .build();

        Dataset baselineDataset = DatasetBuilder.builder()
                                                .type( DataType.SINGLE_VALUED_FORECASTS )
                                                .variable( VariableBuilder.builder()
                                                                          .name( "baseline" )
                                                                          .build() )
                                                .build();

        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( baselineDataset )
                                                         .build();

        Dataset covariate = DatasetBuilder.builder()
                                          .type( DataType.OBSERVATIONS )
                                          .variable( VariableBuilder.builder()
                                                                    .name( VARIABLE_NAME )
                                                                    .build() )
                                          .build();

        CovariateDataset covariateDataset = CovariateDatasetBuilder.builder()
                                                                   .dataset( covariate )
                                                                   .featureNameOrientation( DatasetOrientation.LEFT )
                                                                   .build();

        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( left )
                                            .right( right )
                                            .baseline( baseline )
                                            .covariates( List.of( covariateDataset ) )
                                            .build();

        Mockito.when( project.getDeclaration() )
               .thenReturn( declaration );
        Mockito.when( project.getCovariateDataset( VARIABLE_NAME ) )
               .thenReturn( covariate );

        this.tester = EnsembleRetrieverFactoryInMemory.of( project, store );
    }

    @Test
    void testGetLeftRetriever()
    {
        Supplier<Stream<TimeSeries<Double>>> actual = this.tester.getLeftRetriever( this.features );

        List<TimeSeries<Double>> actualSeries = actual.get()
                                                      .toList();
        List<TimeSeries<Double>> expectedSeries = List.of( this.left );

        assertEquals( expectedSeries, actualSeries );
    }

    @Test
    void testGetRightRetriever()
    {
        TimeWindow innerTimeWindow = MessageUtilities.getTimeWindow( TimeWindowOuter.DURATION_MIN,
                                                                     TimeWindowOuter.DURATION_MAX );
        TimeWindowOuter timeWindow = TimeWindowOuter.of( innerTimeWindow );
        Supplier<Stream<TimeSeries<Ensemble>>> actual = this.tester.getRightRetriever( this.features, timeWindow );

        List<TimeSeries<Ensemble>> actualSeries = actual.get()
                                                        .toList();
        List<TimeSeries<Ensemble>> expectedSeries = List.of( this.right );

        assertEquals( expectedSeries, actualSeries );
    }

    @Test
    void testGetBaselineRetriever()
    {
        TimeWindow innerTimeWindow = MessageUtilities.getTimeWindow( TimeWindowOuter.DURATION_MIN,
                                                                     TimeWindowOuter.DURATION_MAX );
        TimeWindowOuter timeWindow = TimeWindowOuter.of( innerTimeWindow );
        Supplier<Stream<TimeSeries<Ensemble>>> actual = this.tester.getBaselineRetriever( this.features, timeWindow );

        List<TimeSeries<Ensemble>> actualSeries = actual.get()
                                                        .toList();
        List<TimeSeries<Ensemble>> expectedSeries = List.of( this.baseline );

        assertEquals( expectedSeries, actualSeries );
    }

    @Test
    void testGetCovariateRetriever()
    {
        Supplier<Stream<TimeSeries<Double>>> actual = this.tester.getCovariateRetriever( this.features,
                                                                                         VARIABLE_NAME );

        List<TimeSeries<Double>> actualSeries = actual.get()
                                                      .toList();
        List<TimeSeries<Double>> expectedSeries = List.of( this.covariate );

        assertEquals( expectedSeries, actualSeries );
    }
}
