package wres.datamodel.time;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.config.yaml.components.DatasetOrientation;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.types.Ensemble;
import wres.statistics.MessageFactory;
import wres.statistics.generated.ReferenceTime;

/**
 * Tests the {@link TimeSeriesStore}.
 * @author James Brown
 */
class TimeSeriesStoreTest
{
    /** Test instance. */
    private TimeSeriesStore store;

    /** Expected left single-valued series.*/
    private TimeSeries<Double> leftSingleValued;

    /** Expected right single-valued series. */
    private TimeSeries<Double> rightSingleValued;

    /** Expected baseline ensemble series. */
    private TimeSeries<Ensemble> baselineEnsemble;

    /** Expected covariate single-valued series. */
    private TimeSeries<Double> covariateSingleValued;

    /** Expected covariate single-valued series. */
    private TimeSeries<Ensemble> covariateEnsemble;

    /** A geographic feature. */
    private Feature feature;

    @BeforeEach
    void runBeforeEachTest()
    {
        TimeSeriesStore.Builder builder = new TimeSeriesStore.Builder();

        this.feature = Feature.of( MessageFactory.getGeometry( "feature" ) );

        TimeSeriesMetadata leftMetadata = TimeSeriesMetadata.of( Map.of(),
                                                                 TimeScaleOuter.of(),
                                                                 "left",
                                                                 this.feature,
                                                                 "left_unit" );
        this.leftSingleValued = new TimeSeries.Builder<Double>()
                .addEvent( Event.of( Instant.parse( "2123-12-01T06:00:00Z" ), 1.0 ) )
                .setMetadata( leftMetadata )
                .build();

        builder.addSingleValuedSeries( this.leftSingleValued, DatasetOrientation.LEFT );

        TimeSeriesMetadata rightMetadata = TimeSeriesMetadata.of( Map.of( ReferenceTime.ReferenceTimeType.T0,
                                                                          Instant.parse( "2123-12-01T06:00:00Z" ) ),
                                                                  TimeScaleOuter.of(),
                                                                  "right",
                                                                  this.feature,
                                                                  "right_unit" );

        this.rightSingleValued = new TimeSeries.Builder<Double>()
                .addEvent( Event.of( Instant.parse( "2123-12-01T07:00:00Z" ), 2.0 ) )
                .setMetadata( rightMetadata )
                .build();

        builder.addSingleValuedSeries( this.rightSingleValued, DatasetOrientation.RIGHT );

        TimeSeriesMetadata baselineMetadata = TimeSeriesMetadata.of( Map.of( ReferenceTime.ReferenceTimeType.T0,
                                                                             Instant.parse( "2123-12-01T06:00:00Z" ) ),
                                                                     TimeScaleOuter.of(),
                                                                     "baseline",
                                                                     this.feature,
                                                                     "baseline_unit" );

        this.baselineEnsemble = new TimeSeries.Builder<Ensemble>()
                .addEvent( Event.of( Instant.parse( "2123-12-01T07:00:00Z" ),
                                     Ensemble.of( 3.0 ) ) )
                .setMetadata( baselineMetadata )
                .build();

        builder.addEnsembleSeries( this.baselineEnsemble, DatasetOrientation.BASELINE );


        TimeSeriesMetadata covariateMetadata = TimeSeriesMetadata.of( Map.of(),
                                                                      TimeScaleOuter.of(),
                                                                      "covariate",
                                                                      this.feature,
                                                                      "covariate_unit" );

        this.covariateSingleValued = new TimeSeries.Builder<Double>()
                .addEvent( Event.of( Instant.parse( "2123-12-01T07:00:00Z" ), 4.0 ) )
                .setMetadata( covariateMetadata )
                .build();

        builder.addSingleValuedSeries( this.covariateSingleValued, DatasetOrientation.COVARIATE );

        this.covariateEnsemble = this.baselineEnsemble;
        builder.addEnsembleSeries( this.covariateEnsemble, DatasetOrientation.COVARIATE );

        this.store = builder.build();
    }

    @Test
    void testGetLeftSingleValuedSeries()
    {
        assertEquals( List.of( this.leftSingleValued ),
                      this.store.getSingleValuedSeries( DatasetOrientation.LEFT )
                                .toList() );
    }

    @Test
    void testGetRightSingleValuedSeries()
    {
        assertEquals( List.of( this.rightSingleValued ),
                      this.store.getSingleValuedSeries( DatasetOrientation.RIGHT )
                                .toList() );
    }

    @Test
    void testGetBaselineEnsembleSeries()
    {
        assertEquals( List.of( this.baselineEnsemble ),
                      this.store.getEnsembleSeries( DatasetOrientation.BASELINE )
                                .toList() );
    }

    @Test
    void testGetCovariateSingleValuedSeries()
    {
        assertEquals( List.of( this.covariateSingleValued ),
                      this.store.getSingleValuedSeries( DatasetOrientation.COVARIATE )
                                .toList() );
    }

    @Test
    void testGetCovariateEnsembleSeries()
    {
        assertEquals( List.of( this.covariateEnsemble ),
                      this.store.getEnsembleSeries( DatasetOrientation.COVARIATE )
                                .toList() );
    }

    @Test
    void testGetCovariateSingleValuedSeriesForNamedFeatureAndVariable()
    {
        assertEquals( List.of( this.covariateSingleValued ),
                      this.store.getSingleValuedSeries( DatasetOrientation.COVARIATE,
                                                        Set.of( this.feature ),
                                                        "covariate" )
                                .toList() );
    }
}
