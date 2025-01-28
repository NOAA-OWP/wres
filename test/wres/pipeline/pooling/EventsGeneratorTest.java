package wres.pipeline.pooling;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.config.yaml.components.CovariateDataset;
import wres.config.yaml.components.CovariateDatasetBuilder;
import wres.config.yaml.components.CovariatePurpose;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.EventDetection;
import wres.config.yaml.components.EventDetectionBuilder;
import wres.config.yaml.components.EventDetectionCombination;
import wres.config.yaml.components.EventDetectionDataset;
import wres.config.yaml.components.EventDetectionMethod;
import wres.config.yaml.components.EventDetectionParameters;
import wres.config.yaml.components.EventDetectionParametersBuilder;
import wres.config.yaml.components.LeadTimeInterval;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceBuilder;
import wres.config.yaml.components.TimeInterval;
import wres.config.yaml.components.TimeWindowAggregation;
import wres.config.yaml.components.Variable;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeSeriesOfDoubleUpscaler;
import wres.datamodel.time.TimeSeriesUpscaler;
import wres.datamodel.time.TimeWindowOuter;
import wres.eventdetection.EventDetector;
import wres.eventdetection.EventDetectorFactory;
import wres.io.project.Project;
import wres.io.retrieving.RetrieverFactory;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.TimeScale;
import wres.statistics.generated.TimeWindow;

/**
 * Tests the {@link EventsGenerator}.
 *
 * @author James Brown
 */
class EventsGeneratorTest
{
    /** Retriever for left-ish or observed data.*/
    @Mock
    private Supplier<Stream<TimeSeries<Double>>> leftRetriever;

    /** Retriever for right-ish or predicted data.*/
    @Mock
    private Supplier<Stream<TimeSeries<Double>>> rightRetriever;

    /** Retriever for baseline-ish data.*/
    @Mock
    private Supplier<Stream<TimeSeries<Double>>> baselineRetriever;

    /** Retriever for covariate data.*/
    @Mock
    private Supplier<Stream<TimeSeries<Double>>> covariateRetrieverOne;

    /** Retriever for covariate data.*/
    @Mock
    private Supplier<Stream<TimeSeries<Double>>> covariateRetrieverTwo;

    /** Retriever factory.*/
    @Mock
    private RetrieverFactory<Double, Double, Double> retrieverFactory;

    /** The mocks. */
    private AutoCloseable mocks;

    @BeforeEach
    void runBeforeEachTest()
    {
        this.mocks = MockitoAnnotations.openMocks( this );
    }

    @AfterEach
    void runAfterEachTest() throws Exception
    {
        if ( Objects.nonNull( this.mocks ) )
        {
            this.mocks.close();
        }
    }

    @Test
    void testEventDetectionWithIntersectionSelectsTwoJointEventsFromFourMarginalEvents()
    {
        TimeSeriesUpscaler<Double> upscaler = TimeSeriesOfDoubleUpscaler.of();
        EventDetectionParameters parameters = EventDetectionParametersBuilder.builder()
                                                                             .windowSize( Duration.ofHours( 6 ) )
                                                                             .minimumEventDuration( Duration.ZERO )
                                                                             .halfLife( Duration.ofHours( 2 ) )
                                                                             .combination( EventDetectionCombination.INTERSECTION )
                                                                             .build();
        EventDetector detector = EventDetectorFactory.getEventDetector( EventDetectionMethod.REGINA_OGDEN,
                                                                        parameters );
        String measurementUnit = "foo";
        EventsGenerator generator = new EventsGenerator( upscaler,
                                                         upscaler,
                                                         upscaler,
                                                         upscaler,
                                                         measurementUnit,
                                                         detector );

        TimeSeries<Double> timeSeriesOne = this.getTestTimeSeriesWithOffset( Duration.ZERO );

        // Shift the series by one hour, which will eliminate the first event upon intersection, leaving two in total
        // of the four events detected across the two series
        TimeSeries<Double> timeSeriesTwo = this.getTestTimeSeriesWithOffset( Duration.ofHours( 1 ) );

        // Mock a retriever factory
        Mockito.when( this.leftRetriever.get() )
               .thenReturn( Stream.of( timeSeriesOne ) );
        Mockito.when( this.rightRetriever.get() )
               .thenReturn( Stream.of( timeSeriesTwo ) );
        Mockito.when( this.retrieverFactory.getLeftRetriever( Mockito.anySet() ) )
               .thenReturn( this.leftRetriever );
        Mockito.when( this.retrieverFactory.getRightRetriever( Mockito.anySet(), Mockito.any() ) )
               .thenReturn( this.rightRetriever );

        // Mock the sufficient elements of a project with two separate datasets for event detection
        EventDetection eventDeclaration = EventDetectionBuilder.builder()
                                                               .method( EventDetectionMethod.REGINA_OGDEN )
                                                               .parameters( parameters )
                                                               .datasets( Set.of( EventDetectionDataset.OBSERVED,
                                                                                  EventDetectionDataset.PREDICTED ) )
                                                               .build();

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .eventDetection( eventDeclaration )
                                                                        .build();

        Geometry geometry = MessageUtilities.getGeometry( "foo" );
        GeometryTuple geoTuple = MessageUtilities.getGeometryTuple( geometry, geometry, geometry );
        GeometryGroup geoGroup = MessageUtilities.getGeometryGroup( null, geoTuple );
        FeatureGroup groupOne = FeatureGroup.of( geoGroup );

        Project project = Mockito.mock( Project.class );
        Mockito.when( project.getFeatureGroups() )
               .thenReturn( Set.of( groupOne ) );
        Mockito.when( project.getDeclaration() )
               .thenReturn( declaration );

        Set<TimeWindowOuter> actual = generator.doEventDetection( project, groupOne, this.retrieverFactory );

        Instant startOne = Instant.parse( "2079-12-03T08:00:00Z" );
        Instant endOne = Instant.parse( "2079-12-03T10:00:00Z" );

        TimeWindow expectedOne = MessageUtilities.getTimeWindow()
                                                 .toBuilder()
                                                 .setEarliestValidTime( MessageUtilities.getTimestamp( startOne ) )
                                                 .setLatestValidTime( MessageUtilities.getTimestamp( endOne ) )
                                                 .build();

        Instant startTwo = Instant.parse( "2079-12-03T09:00:00Z" );
        Instant endTwo = Instant.parse( "2079-12-03T11:00:00Z" );

        TimeWindow expectedTwo = MessageUtilities.getTimeWindow()
                                                 .toBuilder()
                                                 .setEarliestValidTime( MessageUtilities.getTimestamp( startTwo ) )
                                                 .setLatestValidTime( MessageUtilities.getTimestamp( endTwo ) )
                                                 .build();

        Set<TimeWindowOuter> expected = Set.of( TimeWindowOuter.of( expectedOne ),
                                                TimeWindowOuter.of( expectedTwo ) );

        assertEquals( expected, actual );
    }

    @Test
    void testEventDetectionWithIntersectionSelectsThreeJointEventsFromSixMarginalEvents()
    {
        TimeSeriesUpscaler<Double> upscaler = TimeSeriesOfDoubleUpscaler.of();
        EventDetectionParameters parameters = EventDetectionParametersBuilder.builder()
                                                                             .windowSize( Duration.ofHours( 6 ) )
                                                                             .minimumEventDuration( Duration.ZERO )
                                                                             .halfLife( Duration.ofHours( 2 ) )
                                                                             .combination( EventDetectionCombination.INTERSECTION )
                                                                             .build();
        EventDetector detector = EventDetectorFactory.getEventDetector( EventDetectionMethod.REGINA_OGDEN,
                                                                        parameters );
        String measurementUnit = "foo";
        EventsGenerator generator = new EventsGenerator( upscaler,
                                                         upscaler,
                                                         upscaler,
                                                         upscaler,
                                                         measurementUnit,
                                                         detector );

        TimeSeries<Double> timeSeriesOne = this.getTestTimeSeriesWithOffset( Duration.ZERO );

        // Shift the series by one hour, which will eliminate the first event upon intersection, leaving two in total
        // of the four events detected across the two series
        TimeSeries<Double> timeSeriesTwo = this.getTestTimeSeriesWithOffset( Duration.ofHours( 1 ) );

        // Shift the baseline by one hour, which will eliminate the first event upon intersection, retaining three
        // of the six events detected across three series
        TimeSeries<Double> timeSeriesThree = this.getTestTimeSeriesWithOffset( Duration.ofHours( -1 ) );

        // Mock a retriever factory
        Mockito.when( this.leftRetriever.get() )
               .thenReturn( Stream.of( timeSeriesOne ) );
        Mockito.when( this.rightRetriever.get() )
               .thenReturn( Stream.of( timeSeriesTwo ) );
        Mockito.when( this.baselineRetriever.get() )
               .thenReturn( Stream.of( timeSeriesThree ) );
        Mockito.when( this.retrieverFactory.getLeftRetriever( Mockito.anySet() ) )
               .thenReturn( this.leftRetriever );
        Mockito.when( this.retrieverFactory.getRightRetriever( Mockito.anySet(), Mockito.any() ) )
               .thenReturn( this.rightRetriever );
        Mockito.when( this.retrieverFactory.getBaselineRetriever( Mockito.anySet(), Mockito.any() ) )
               .thenReturn( this.baselineRetriever );

        // Mock the sufficient elements of a project with three separate datasets for event detection
        EventDetection eventDeclaration = EventDetectionBuilder.builder()
                                                               .method( EventDetectionMethod.REGINA_OGDEN )
                                                               .parameters( parameters )
                                                               .datasets( Set.of( EventDetectionDataset.OBSERVED,
                                                                                  EventDetectionDataset.PREDICTED,
                                                                                  EventDetectionDataset.BASELINE ) )
                                                               .build();

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .eventDetection( eventDeclaration )
                                                                        .build();

        Geometry geometry = MessageUtilities.getGeometry( "foo" );
        GeometryTuple geoTuple = MessageUtilities.getGeometryTuple( geometry, geometry, geometry );
        GeometryGroup geoGroup = MessageUtilities.getGeometryGroup( null, geoTuple );
        FeatureGroup groupOne = FeatureGroup.of( geoGroup );

        Project project = Mockito.mock( Project.class );
        Mockito.when( project.getFeatureGroups() )
               .thenReturn( Set.of( groupOne ) );
        Mockito.when( project.getDeclaration() )
               .thenReturn( declaration );

        Set<TimeWindowOuter> actual = generator.doEventDetection( project, groupOne, this.retrieverFactory );

        Instant startOne = Instant.parse( "2079-12-03T08:00:00Z" );
        Instant endOne = Instant.parse( "2079-12-03T10:00:00Z" );

        TimeWindow expectedOne = MessageUtilities.getTimeWindow()
                                                 .toBuilder()
                                                 .setEarliestValidTime( MessageUtilities.getTimestamp( startOne ) )
                                                 .setLatestValidTime( MessageUtilities.getTimestamp( endOne ) )
                                                 .build();

        Instant startTwo = Instant.parse( "2079-12-03T09:00:00Z" );
        Instant endTwo = Instant.parse( "2079-12-03T11:00:00Z" );

        TimeWindow expectedTwo = MessageUtilities.getTimeWindow()
                                                 .toBuilder()
                                                 .setEarliestValidTime( MessageUtilities.getTimestamp( startTwo ) )
                                                 .setLatestValidTime( MessageUtilities.getTimestamp( endTwo ) )
                                                 .build();

        Instant startThree = Instant.parse( "2079-12-03T07:00:00Z" );
        Instant endThree = Instant.parse( "2079-12-03T09:00:00Z" );

        TimeWindow expectedThree = MessageUtilities.getTimeWindow()
                                                   .toBuilder()
                                                   .setEarliestValidTime( MessageUtilities.getTimestamp( startThree ) )
                                                   .setLatestValidTime( MessageUtilities.getTimestamp( endThree ) )
                                                   .build();

        Set<TimeWindowOuter> expected = Set.of( TimeWindowOuter.of( expectedOne ),
                                                TimeWindowOuter.of( expectedTwo ),
                                                TimeWindowOuter.of( expectedThree ) );

        assertEquals( expected, actual );
    }

    @Test
    void testEventDetectionWithIntersectionGeneratesOneAggregateEventFromSixMarginalEvents()
    {
        TimeSeriesUpscaler<Double> upscaler = TimeSeriesOfDoubleUpscaler.of();
        EventDetectionParameters parameters = EventDetectionParametersBuilder.builder()
                                                                             .windowSize( Duration.ofHours( 6 ) )
                                                                             .minimumEventDuration( Duration.ZERO )
                                                                             .halfLife( Duration.ofHours( 2 ) )
                                                                             .combination( EventDetectionCombination.INTERSECTION )
                                                                             .aggregation( TimeWindowAggregation.MAXIMUM )
                                                                             .build();
        EventDetector detector = EventDetectorFactory.getEventDetector( EventDetectionMethod.REGINA_OGDEN,
                                                                        parameters );
        String measurementUnit = "foo";
        EventsGenerator generator = new EventsGenerator( upscaler,
                                                         upscaler,
                                                         upscaler,
                                                         upscaler,
                                                         measurementUnit,
                                                         detector );

        TimeSeries<Double> timeSeriesOne = this.getTestTimeSeriesWithOffset( Duration.ZERO );

        // Shift the series by one hour, which will eliminate the first event upon intersection, leaving two in total
        // of the four events detected across the two series
        TimeSeries<Double> timeSeriesTwo = this.getTestTimeSeriesWithOffset( Duration.ofHours( 1 ) );

        // Shift the baseline by one hour, which will eliminate the first event upon intersection, retaining three
        // of the six events detected across three series
        TimeSeries<Double> timeSeriesThree = this.getTestTimeSeriesWithOffset( Duration.ofHours( -1 ) );

        // Mock a retriever factory
        Mockito.when( this.leftRetriever.get() )
               .thenReturn( Stream.of( timeSeriesOne ) );
        Mockito.when( this.rightRetriever.get() )
               .thenReturn( Stream.of( timeSeriesTwo ) );
        Mockito.when( this.baselineRetriever.get() )
               .thenReturn( Stream.of( timeSeriesThree ) );
        Mockito.when( this.retrieverFactory.getLeftRetriever( Mockito.anySet() ) )
               .thenReturn( this.leftRetriever );
        Mockito.when( this.retrieverFactory.getRightRetriever( Mockito.anySet(), Mockito.any() ) )
               .thenReturn( this.rightRetriever );
        Mockito.when( this.retrieverFactory.getBaselineRetriever( Mockito.anySet(), Mockito.any() ) )
               .thenReturn( this.baselineRetriever );

        // Mock the sufficient elements of a project with three separate datasets for event detection
        EventDetection eventDeclaration = EventDetectionBuilder.builder()
                                                               .method( EventDetectionMethod.REGINA_OGDEN )
                                                               .parameters( parameters )
                                                               .datasets( Set.of( EventDetectionDataset.OBSERVED,
                                                                                  EventDetectionDataset.PREDICTED,
                                                                                  EventDetectionDataset.BASELINE ) )
                                                               .build();

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .eventDetection( eventDeclaration )
                                                                        .build();

        Geometry geometry = MessageUtilities.getGeometry( "foo" );
        GeometryTuple geoTuple = MessageUtilities.getGeometryTuple( geometry, geometry, geometry );
        GeometryGroup geoGroup = MessageUtilities.getGeometryGroup( null, geoTuple );
        FeatureGroup groupOne = FeatureGroup.of( geoGroup );

        Project project = Mockito.mock( Project.class );
        Mockito.when( project.getFeatureGroups() )
               .thenReturn( Set.of( groupOne ) );
        Mockito.when( project.getDeclaration() )
               .thenReturn( declaration );

        Set<TimeWindowOuter> actual = generator.doEventDetection( project, groupOne, this.retrieverFactory );

        Instant startOne = Instant.parse( "2079-12-03T07:00:00Z" );
        Instant endOne = Instant.parse( "2079-12-03T11:00:00Z" );

        TimeWindow expectedOne = MessageUtilities.getTimeWindow()
                                                 .toBuilder()
                                                 .setEarliestValidTime( MessageUtilities.getTimestamp( startOne ) )
                                                 .setLatestValidTime( MessageUtilities.getTimestamp( endOne ) )
                                                 .build();

        Set<TimeWindowOuter> expected = Set.of( TimeWindowOuter.of( expectedOne ) );

        assertEquals( expected, actual );
    }

    @Test
    void testEventDetectionWithIntersectionSelectsThreeJointEventsFromSixMarginalEventsWithCovariates()
    {
        TimeSeriesUpscaler<Double> upscaler = TimeSeriesOfDoubleUpscaler.of();
        EventDetectionParameters parameters = EventDetectionParametersBuilder.builder()
                                                                             .windowSize( Duration.ofHours( 6 ) )
                                                                             .minimumEventDuration( Duration.ZERO )
                                                                             .halfLife( Duration.ofHours( 2 ) )
                                                                             .combination( EventDetectionCombination.INTERSECTION )
                                                                             .build();
        EventDetector detector = EventDetectorFactory.getEventDetector( EventDetectionMethod.REGINA_OGDEN,
                                                                        parameters );
        String measurementUnit = "foo";
        EventsGenerator generator = new EventsGenerator( upscaler,
                                                         upscaler,
                                                         upscaler,
                                                         upscaler,
                                                         measurementUnit,
                                                         detector );

        TimeSeries<Double> timeSeriesOne = this.getTestTimeSeriesWithOffset( Duration.ZERO );

        // Shift the series by one hour, which will eliminate the first event upon intersection, leaving two in total
        // of the four events detected across the two series
        TimeSeries<Double> timeSeriesTwo = this.getTestTimeSeriesWithOffset( Duration.ofHours( 1 ) );

        // Shift the baseline by one hour, which will eliminate the first event upon intersection, retaining three
        // of the six events detected across three series
        TimeSeries<Double> timeSeriesThree = this.getTestTimeSeriesWithOffset( Duration.ofHours( -1 ) );

        // Mock a retriever factory
        Mockito.when( this.leftRetriever.get() )
               .thenReturn( Stream.of( timeSeriesOne ) );
        Mockito.when( this.covariateRetrieverOne.get() )
               .thenReturn( Stream.of( timeSeriesTwo ) );
        Mockito.when( this.covariateRetrieverTwo.get() )
               .thenReturn( Stream.of( timeSeriesThree ) );
        Mockito.when( this.retrieverFactory.getLeftRetriever( Mockito.anySet() ) )
               .thenReturn( this.leftRetriever );
        Mockito.when( this.retrieverFactory.getCovariateRetriever( Mockito.anySet(),
                                                                   ArgumentMatchers.eq( "qux" ) ) )
               .thenReturn( this.covariateRetrieverOne );
        Mockito.when( this.retrieverFactory.getCovariateRetriever( Mockito.anySet(),
                                                                   ArgumentMatchers.eq( "quux" ) ) )
               .thenReturn( this.covariateRetrieverTwo );

        // Mock the sufficient elements of a project with three separate datasets for event detection
        EventDetection eventDeclaration = EventDetectionBuilder.builder()
                                                               .method( EventDetectionMethod.REGINA_OGDEN )
                                                               .parameters( parameters )
                                                               .datasets( Set.of( EventDetectionDataset.OBSERVED,
                                                                                  EventDetectionDataset.COVARIATES ) )
                                                               .build();

        URI covariateOneUri = URI.create( "qux.tgz" );
        Source covariateOneSource = SourceBuilder.builder()
                                                 .uri( covariateOneUri )
                                                 .build();

        List<Source> covariateOneSources = List.of( covariateOneSource );

        Dataset covariateOneDataset = DatasetBuilder.builder()
                                                    .sources( covariateOneSources )
                                                    .variable( new Variable( "qux", null, Set.of() ) )
                                                    .build();
        CovariateDataset covariateOne = CovariateDatasetBuilder.builder()
                                                               .dataset( covariateOneDataset )
                                                               .minimum( 0.25 )
                                                               .purposes( Set.of( CovariatePurpose.DETECT ) )
                                                               .featureNameOrientation( DatasetOrientation.LEFT )
                                                               .build();

        URI covariateTwoUri = URI.create( "quux.tgz" );
        Source covariateTwoSource = SourceBuilder.builder()
                                                 .uri( covariateTwoUri )
                                                 .build();

        List<Source> covariateTwoSources = List.of( covariateTwoSource );

        Dataset covariateTwoDataset = DatasetBuilder.builder()
                                                    .sources( covariateTwoSources )
                                                    .variable( new Variable( "quux", null, Set.of() ) )
                                                    .build();

        CovariateDataset covariateTwo = CovariateDatasetBuilder.builder()
                                                               .dataset( covariateTwoDataset )
                                                               .maximum( 0.0 )
                                                               .rescaleFunction( TimeScale.TimeScaleFunction.MEAN )
                                                               .purposes( Set.of( CovariatePurpose.DETECT ) )
                                                               .featureNameOrientation( DatasetOrientation.LEFT )
                                                               .build();

        List<CovariateDataset> covariateDatasets = List.of( covariateOne, covariateTwo );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .eventDetection( eventDeclaration )
                                                                        .covariates( covariateDatasets )
                                                                        .build();

        Geometry geometry = MessageUtilities.getGeometry( "foo" );
        GeometryTuple geoTuple = MessageUtilities.getGeometryTuple( geometry, geometry, geometry );
        GeometryGroup geoGroup = MessageUtilities.getGeometryGroup( null, geoTuple );
        FeatureGroup groupOne = FeatureGroup.of( geoGroup );

        Project project = Mockito.mock( Project.class );
        Mockito.when( project.getFeatureGroups() )
               .thenReturn( Set.of( groupOne ) );
        Mockito.when( project.getDeclaration() )
               .thenReturn( declaration );

        Set<TimeWindowOuter> actual = generator.doEventDetection( project, groupOne, this.retrieverFactory );

        Instant startOne = Instant.parse( "2079-12-03T08:00:00Z" );
        Instant endOne = Instant.parse( "2079-12-03T10:00:00Z" );

        TimeWindow expectedOne = MessageUtilities.getTimeWindow()
                                                 .toBuilder()
                                                 .setEarliestValidTime( MessageUtilities.getTimestamp( startOne ) )
                                                 .setLatestValidTime( MessageUtilities.getTimestamp( endOne ) )
                                                 .build();

        Instant startTwo = Instant.parse( "2079-12-03T09:00:00Z" );
        Instant endTwo = Instant.parse( "2079-12-03T11:00:00Z" );

        TimeWindow expectedTwo = MessageUtilities.getTimeWindow()
                                                 .toBuilder()
                                                 .setEarliestValidTime( MessageUtilities.getTimestamp( startTwo ) )
                                                 .setLatestValidTime( MessageUtilities.getTimestamp( endTwo ) )
                                                 .build();

        Instant startThree = Instant.parse( "2079-12-03T07:00:00Z" );
        Instant endThree = Instant.parse( "2079-12-03T09:00:00Z" );

        TimeWindow expectedThree = MessageUtilities.getTimeWindow()
                                                   .toBuilder()
                                                   .setEarliestValidTime( MessageUtilities.getTimestamp( startThree ) )
                                                   .setLatestValidTime( MessageUtilities.getTimestamp( endThree ) )
                                                   .build();

        Set<TimeWindowOuter> expected = Set.of( TimeWindowOuter.of( expectedOne ),
                                                TimeWindowOuter.of( expectedTwo ),
                                                TimeWindowOuter.of( expectedThree ) );

        assertEquals( expected, actual );
    }

    @Test
    void testEventDetectionWithUnionSelectsFourJointEventsFromFourMarginalEvents()
    {
        TimeSeriesUpscaler<Double> upscaler = TimeSeriesOfDoubleUpscaler.of();
        EventDetectionParameters parameters = EventDetectionParametersBuilder.builder()
                                                                             .windowSize( Duration.ofHours( 6 ) )
                                                                             .minimumEventDuration( Duration.ZERO )
                                                                             .halfLife( Duration.ofHours( 2 ) )
                                                                             .combination( EventDetectionCombination.UNION )
                                                                             .build();
        EventDetector detector = EventDetectorFactory.getEventDetector( EventDetectionMethod.REGINA_OGDEN,
                                                                        parameters );
        String measurementUnit = "foo";
        EventsGenerator generator = new EventsGenerator( upscaler,
                                                         upscaler,
                                                         upscaler,
                                                         upscaler,
                                                         measurementUnit,
                                                         detector );

        TimeSeries<Double> timeSeriesOne = this.getTestTimeSeriesWithOffset( Duration.ZERO );

        // Shift the series by one hour, which will eliminate the first event upon intersection, leaving two in total
        // of the four events detected across the two series
        TimeSeries<Double> timeSeriesTwo = this.getTestTimeSeriesWithOffset( Duration.ofHours( 1 ) );

        // Mock a retriever factory
        Mockito.when( this.leftRetriever.get() )
               .thenReturn( Stream.of( timeSeriesOne ) );
        Mockito.when( this.rightRetriever.get() )
               .thenReturn( Stream.of( timeSeriesTwo ) );
        Mockito.when( this.retrieverFactory.getLeftRetriever( Mockito.anySet() ) )
               .thenReturn( this.leftRetriever );
        Mockito.when( this.retrieverFactory.getRightRetriever( Mockito.anySet(), Mockito.any() ) )
               .thenReturn( this.rightRetriever );

        // Mock the sufficient elements of a project with two separate datasets for event detection
        EventDetection eventDeclaration = EventDetectionBuilder.builder()
                                                               .method( EventDetectionMethod.REGINA_OGDEN )
                                                               .parameters( parameters )
                                                               .datasets( Set.of( EventDetectionDataset.OBSERVED,
                                                                                  EventDetectionDataset.PREDICTED ) )
                                                               .build();

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .eventDetection( eventDeclaration )
                                                                        .build();

        Geometry geometry = MessageUtilities.getGeometry( "foo" );
        GeometryTuple geoTuple = MessageUtilities.getGeometryTuple( geometry, geometry, geometry );
        GeometryGroup geoGroup = MessageUtilities.getGeometryGroup( null, geoTuple );
        FeatureGroup groupOne = FeatureGroup.of( geoGroup );

        Project project = Mockito.mock( Project.class );
        Mockito.when( project.getFeatureGroups() )
               .thenReturn( Set.of( groupOne ) );
        Mockito.when( project.getDeclaration() )
               .thenReturn( declaration );

        Set<TimeWindowOuter> actual = generator.doEventDetection( project, groupOne, this.retrieverFactory );

        Instant startOne = Instant.parse( "2079-12-03T08:00:00Z" );
        Instant endOne = Instant.parse( "2079-12-03T10:00:00Z" );

        TimeWindow expectedOne = MessageUtilities.getTimeWindow()
                                                 .toBuilder()
                                                 .setEarliestValidTime( MessageUtilities.getTimestamp( startOne ) )
                                                 .setLatestValidTime( MessageUtilities.getTimestamp( endOne ) )
                                                 .build();

        Instant startTwo = Instant.parse( "2079-12-03T09:00:00Z" );
        Instant endTwo = Instant.parse( "2079-12-03T11:00:00Z" );

        TimeWindow expectedTwo = MessageUtilities.getTimeWindow()
                                                 .toBuilder()
                                                 .setEarliestValidTime( MessageUtilities.getTimestamp( startTwo ) )
                                                 .setLatestValidTime( MessageUtilities.getTimestamp( endTwo ) )
                                                 .build();

        Instant startThree = Instant.parse( "2079-12-03T03:00:00Z" );
        Instant endThree = Instant.parse( "2079-12-03T03:00:00Z" );

        TimeWindow expectedThree = MessageUtilities.getTimeWindow()
                                                   .toBuilder()
                                                   .setEarliestValidTime( MessageUtilities.getTimestamp( startThree ) )
                                                   .setLatestValidTime( MessageUtilities.getTimestamp( endThree ) )
                                                   .build();

        Instant startFour = Instant.parse( "2079-12-03T04:00:00Z" );
        Instant endFour = Instant.parse( "2079-12-03T04:00:00Z" );

        TimeWindow expectedFour = MessageUtilities.getTimeWindow()
                                                  .toBuilder()
                                                  .setEarliestValidTime( MessageUtilities.getTimestamp( startFour ) )
                                                  .setLatestValidTime( MessageUtilities.getTimestamp( endFour ) )
                                                  .build();

        Set<TimeWindowOuter> expected = Set.of( TimeWindowOuter.of( expectedOne ),
                                                TimeWindowOuter.of( expectedTwo ),
                                                TimeWindowOuter.of( expectedThree ),
                                                TimeWindowOuter.of( expectedFour ) );

        assertEquals( expected, actual );
    }

    @Test
    void testEventDetectionAddsDeclaredTimeConstraintsToDetectedEvent()
    {
        TimeSeriesUpscaler<Double> upscaler = TimeSeriesOfDoubleUpscaler.of();
        EventDetectionParameters parameters = EventDetectionParametersBuilder.builder()
                                                                             .build();
        EventDetector detector = EventDetectorFactory.getEventDetector( EventDetectionMethod.REGINA_OGDEN,
                                                                        parameters );
        String measurementUnit = "foo";
        EventsGenerator generator = new EventsGenerator( upscaler,
                                                         upscaler,
                                                         upscaler,
                                                         upscaler,
                                                         measurementUnit,
                                                         detector );

        TimeSeries<Double> timeSeriesOne = this.getTestTimeSeriesWithOffset( Duration.ZERO );

        // Mock a retriever factory
        Mockito.when( this.leftRetriever.get() )
               .thenReturn( Stream.of( timeSeriesOne ) );
        Mockito.when( this.retrieverFactory.getLeftRetriever( Mockito.anySet() ) )
               .thenReturn( this.leftRetriever );

        // Mock the sufficient elements of a project with two separate datasets for event detection
        EventDetection eventDeclaration = EventDetectionBuilder.builder()
                                                               .method( EventDetectionMethod.REGINA_OGDEN )
                                                               .parameters( parameters )
                                                               .datasets( Set.of( EventDetectionDataset.OBSERVED ) )
                                                               .build();
        // Reference time constraints
        Instant earliestReference = Instant.parse( "2099-10-21T00:00:00Z" );
        Instant latestReference = Instant.parse( "2101-10-21T00:00:00Z" );
        TimeInterval referenceTimes = new TimeInterval( earliestReference, latestReference );

        // Lead time constraints
        Duration earliestLead = Duration.ofHours( 23 );
        Duration latestLead = Duration.ofHours( 79 );
        LeadTimeInterval leadTimes = new LeadTimeInterval( earliestLead, latestLead );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .eventDetection( eventDeclaration )
                                                                        .referenceDates( referenceTimes )
                                                                        .leadTimes( leadTimes )
                                                                        .build();

        Geometry geometry = MessageUtilities.getGeometry( "foo" );
        GeometryTuple geoTuple = MessageUtilities.getGeometryTuple( geometry, geometry, geometry );
        GeometryGroup geoGroup = MessageUtilities.getGeometryGroup( null, geoTuple );
        FeatureGroup groupOne = FeatureGroup.of( geoGroup );

        Project project = Mockito.mock( Project.class );
        Mockito.when( project.getFeatureGroups() )
               .thenReturn( Set.of( groupOne ) );
        Mockito.when( project.getDeclaration() )
               .thenReturn( declaration );

        Set<TimeWindowOuter> actual = generator.doEventDetection( project, groupOne, this.retrieverFactory );

        Instant startOne = Instant.parse( "2079-12-03T03:00:00Z" );
        Instant endOne = Instant.parse( "2079-12-03T05:00:00Z" );

        TimeWindow expectedOne =
                MessageUtilities.getTimeWindow()
                                .toBuilder()
                                .setEarliestValidTime( MessageUtilities.getTimestamp( startOne ) )
                                .setLatestValidTime( MessageUtilities.getTimestamp( endOne ) )
                                .setEarliestReferenceTime( MessageUtilities.getTimestamp( earliestReference ) )
                                .setLatestReferenceTime( MessageUtilities.getTimestamp( latestReference ) )
                                .setEarliestLeadDuration( MessageUtilities.getDuration( earliestLead ) )
                                .setLatestLeadDuration( MessageUtilities.getDuration( latestLead ) )
                                .build();

        Instant startTwo = Instant.parse( "2079-12-03T09:00:00Z" );
        Instant endTwo = Instant.parse( "2079-12-03T10:00:00Z" );

        TimeWindow expectedTwo =
                MessageUtilities.getTimeWindow()
                                .toBuilder()
                                .setEarliestValidTime( MessageUtilities.getTimestamp( startTwo ) )
                                .setLatestValidTime( MessageUtilities.getTimestamp( endTwo ) )
                                .setEarliestReferenceTime( MessageUtilities.getTimestamp( earliestReference ) )
                                .setLatestReferenceTime( MessageUtilities.getTimestamp( latestReference ) )
                                .setEarliestLeadDuration( MessageUtilities.getDuration( earliestLead ) )
                                .setLatestLeadDuration( MessageUtilities.getDuration( latestLead ) )
                                .build();

        Set<TimeWindowOuter> expected = Set.of( TimeWindowOuter.of( expectedOne ),
                                                TimeWindowOuter.of( expectedTwo ) );

        assertEquals( expected, actual );
    }

    /**
     * Generates a test time-series.
     * @param offset the offset to apply
     * @return the test series
     */

    private TimeSeries<Double> getTestTimeSeriesWithOffset( Duration offset )
    {
        Event<Double> one = Event.of( Instant.parse( "2079-12-03T00:00:00Z" )
                                             .plus( offset ), 5.0 );
        Event<Double> two = Event.of( Instant.parse( "2079-12-03T01:00:00Z" )
                                             .plus( offset ), 5.0 );
        Event<Double> three = Event.of( Instant.parse( "2079-12-03T02:00:00Z" )
                                               .plus( offset ), 24.0 );
        Event<Double> four = Event.of( Instant.parse( "2079-12-03T03:00:00Z" )
                                              .plus( offset ), 25.0 );
        Event<Double> five = Event.of( Instant.parse( "2079-12-03T04:00:00Z" )
                                              .plus( offset ), 5.0 );
        Event<Double> six = Event.of( Instant.parse( "2079-12-03T05:00:00Z" )
                                             .plus( offset ), 5.0 );
        Event<Double> seven = Event.of( Instant.parse( "2079-12-03T06:00:00Z" )
                                               .plus( offset ), 5.0 );
        Event<Double> eight = Event.of( Instant.parse( "2079-12-03T07:00:00Z" )
                                               .plus( offset ), 84.0 );
        Event<Double> nine = Event.of( Instant.parse( "2079-12-03T08:00:00Z" )
                                              .plus( offset ), 85.0 );
        Event<Double> ten = Event.of( Instant.parse( "2079-12-03T09:00:00Z" )
                                             .plus( offset ), 87.0 );
        Event<Double> eleven = Event.of( Instant.parse( "2079-12-03T10:00:00Z" )
                                                .plus( offset ), 5.0 );
        Event<Double> twelve = Event.of( Instant.parse( "2079-12-03T11:00:00Z" )
                                                .plus( offset ), 5.0 );

        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of(),
                                                             TimeScaleOuter.of(),
                                                             "foo",
                                                             Feature.of( MessageUtilities.getGeometry( "bar" ) ),
                                                             "baz" );
        return new TimeSeries.Builder<Double>()
                .addEvent( one )
                .addEvent( two )
                .addEvent( three )
                .addEvent( four )
                .addEvent( five )
                .addEvent( six )
                .addEvent( seven )
                .addEvent( eight )
                .addEvent( nine )
                .addEvent( ten )
                .addEvent( eleven )
                .addEvent( twelve )
                .setMetadata( metadata )
                .build();
    }
}