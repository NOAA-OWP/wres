package wres.datamodel.messages;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.google.protobuf.DoubleValue;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;

import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Pairs;
import wres.statistics.generated.Season;
import wres.statistics.generated.Threshold;
import wres.statistics.generated.TimeScale;
import wres.statistics.generated.TimeWindow;
import wres.statistics.generated.ValueFilter;
import wres.statistics.generated.Outputs.CsvFormat;
import wres.statistics.generated.Outputs.GraphicFormat;
import wres.statistics.generated.Outputs.NetcdfFormat;
import wres.statistics.generated.Outputs.NumericFormat;
import wres.statistics.generated.Outputs.PngFormat;
import wres.statistics.generated.Outputs.ProtobufFormat;
import wres.statistics.generated.Outputs.SvgFormat;
import wres.statistics.generated.Pool;
import wres.statistics.generated.ReferenceTime;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;
import wres.statistics.generated.Outputs.GraphicFormat.DurationUnit;
import wres.statistics.generated.Outputs.GraphicFormat.GraphicShape;
import wres.statistics.generated.Pairs.Pair;
import wres.statistics.generated.Pairs.TimeSeriesOfPairs;

/**
 * Tests the {@link MessageUtilities}.
 * 
 * @author james.brown@hydrosolved.com
 */

class MessageUtilitiesTest
{

    @Test
    void testCompareEvaluation()
    {
        // Consistent with equals

        // Compare default instances
        assertEquals( 0, MessageUtilities.compare( Evaluation.getDefaultInstance(), Evaluation.getDefaultInstance() ) );
        // Compare full instance
        Evaluation zeroth = Evaluation.newBuilder()
                                      .addEnsembleMemberSubset( "aSubset" )
                                      .setLeftDataName( "leftData" )
                                      .setLeftVariableName( "leftVariable" )
                                      .setRightDataName( "rightData" )
                                      .setRightDataName( "rightVariable" )
                                      .setBaselineDataName( "baselineData" )
                                      .setBaselineVariableName( "baselineVariable" )
                                      .setMeasurementUnit( "measurementUnit" )
                                      .setMetricCount( 1 )
                                      .setSeason( Season.newBuilder()
                                                        .setStartDay( 1 )
                                                        .setStartMonth( 1 )
                                                        .setEndDay( 2 )
                                                        .setEndMonth( 2 ) )
                                      .setOutputs( Outputs.newBuilder()
                                                          .setCsv( CsvFormat.newBuilder()
                                                                            .setOptions( NumericFormat.newBuilder()
                                                                                                      .setDecimalFormat( "0.0" ) ) )
                                                          .setPng( PngFormat.newBuilder()
                                                                            .setOptions( GraphicFormat.newBuilder()
                                                                                                      .setConfiguration( "someConfiguration" )
                                                                                                      .setHeight( 600 )
                                                                                                      .setWidth( 800 )
                                                                                                      .addIgnore( MetricName.BIAS_FRACTION )
                                                                                                      .setLeadUnit( DurationUnit.HOURS )
                                                                                                      .setShape( GraphicShape.ISSUED_DATE_POOLS )
                                                                                                      .setTemplateName( "aTemplate" ) ) )
                                                          .setSvg( SvgFormat.newBuilder()
                                                                            .setOptions( GraphicFormat.newBuilder()
                                                                                                      .setConfiguration( "moreConfiguration" )
                                                                                                      .setHeight( 600 )
                                                                                                      .setWidth( 800 )
                                                                                                      .addIgnore( MetricName.BOX_PLOT_OF_ERRORS )
                                                                                                      .setLeadUnit( DurationUnit.MINUTES )
                                                                                                      .setShape( GraphicShape.LEAD_THRESHOLD )
                                                                                                      .setTemplateName( "anotherTemplate" ) ) )
                                                          .setProtobuf( ProtobufFormat.newBuilder() )
                                                          .setNetcdf( NetcdfFormat.newBuilder() ) )
                                      .setValueFilter( ValueFilter.newBuilder()
                                                                  .setMinimumInclusiveValue( 0.0 )
                                                                  .setMinimumInclusiveValue( 1.0 ) )
                                      .build();

        Evaluation first = zeroth.toBuilder()
                                 .build();

        assertEquals( zeroth, zeroth );
        assertEquals( zeroth, first );
        assertEquals( 0, MessageUtilities.compare( zeroth, zeroth ) );
        assertEquals( 0, MessageUtilities.compare( zeroth, first ) );

        // Less than and greater than examples
        Evaluation second = Evaluation.newBuilder()
                                      .addEnsembleMemberSubset( "1" )
                                      .build();

        Evaluation third = Evaluation.newBuilder()
                                     .addEnsembleMemberSubset( "2" )
                                     .build();

        assertTrue( MessageUtilities.compare( second, third ) < 0 );

        Evaluation fourth = Evaluation.newBuilder()
                                      .setLeftDataName( "1" )
                                      .build();

        Evaluation fifth = Evaluation.newBuilder()
                                     .setLeftDataName( "2" )
                                     .build();

        assertTrue( MessageUtilities.compare( fourth, fifth ) < 0 );
        assertTrue( MessageUtilities.compare( fifth, fourth ) > 0 );

        Evaluation sixth = Evaluation.newBuilder()
                                     .setLeftVariableName( "1" )
                                     .build();

        Evaluation seventh = Evaluation.newBuilder()
                                       .setLeftVariableName( "2" )
                                       .build();

        assertTrue( MessageUtilities.compare( sixth, seventh ) < 0 );

        Evaluation eighth = Evaluation.newBuilder()
                                      .setRightDataName( "1" )
                                      .build();

        Evaluation ninth = Evaluation.newBuilder()
                                     .setRightDataName( "2" )
                                     .build();

        assertTrue( MessageUtilities.compare( eighth, ninth ) < 0 );

        Evaluation tenth = Evaluation.newBuilder()
                                     .setRightVariableName( "1" )
                                     .build();

        Evaluation eleventh = Evaluation.newBuilder()
                                        .setRightVariableName( "2" )
                                        .build();

        assertTrue( MessageUtilities.compare( tenth, eleventh ) < 0 );

        Evaluation twelfth = Evaluation.newBuilder()
                                       .setBaselineDataName( "1" )
                                       .build();

        Evaluation thirteenth = Evaluation.newBuilder()
                                          .setBaselineDataName( "2" )
                                          .build();

        assertTrue( MessageUtilities.compare( twelfth, thirteenth ) < 0 );

        Evaluation fourteenth = Evaluation.newBuilder()
                                          .setBaselineVariableName( "1" )
                                          .build();

        Evaluation fifteenth = Evaluation.newBuilder()
                                         .setBaselineVariableName( "2" )
                                         .build();

        assertTrue( MessageUtilities.compare( fourteenth, fifteenth ) < 0 );

        Evaluation sixteenth = Evaluation.newBuilder()
                                         .setMeasurementUnit( "1" )
                                         .build();

        Evaluation seventeenth = Evaluation.newBuilder()
                                           .setMeasurementUnit( "2" )
                                           .build();

        assertTrue( MessageUtilities.compare( sixteenth, seventeenth ) < 0 );

        Evaluation eighteenth = Evaluation.newBuilder()
                                          .setMeasurementUnit( "1" )
                                          .build();

        Evaluation nineteenth = Evaluation.newBuilder()
                                          .setMeasurementUnit( "1" )
                                          .setOutputs( Outputs.newBuilder()
                                                              .setCsv( CsvFormat.newBuilder()
                                                                                .setOptions( NumericFormat.newBuilder()
                                                                                                          .setDecimalFormat( "0.0" ) ) ) )
                                          .build();

        assertTrue( MessageUtilities.compare( eighteenth, nineteenth ) < 0 );
    }

    @Test
    void testCompareOutputs()
    {
        // Consistent with equals

        // Compare default instances
        assertEquals( 0, MessageUtilities.compare( Outputs.getDefaultInstance(), Outputs.getDefaultInstance() ) );
        // Compare full instance    
        Outputs zeroth = Outputs.newBuilder()
                                .setCsv( CsvFormat.newBuilder()
                                                  .setOptions( NumericFormat.newBuilder()
                                                                            .setDecimalFormat( "0.0" ) ) )
                                .setPng( PngFormat.newBuilder()
                                                  .setOptions( GraphicFormat.newBuilder()
                                                                            .setConfiguration( "someConfiguration" )
                                                                            .setHeight( 600 )
                                                                            .setWidth( 800 )
                                                                            .addIgnore( MetricName.BIAS_FRACTION )
                                                                            .setLeadUnit( DurationUnit.HOURS )
                                                                            .setShape( GraphicShape.ISSUED_DATE_POOLS )
                                                                            .setTemplateName( "aTemplate" ) ) )
                                .setSvg( SvgFormat.newBuilder()
                                                  .setOptions( GraphicFormat.newBuilder()
                                                                            .setConfiguration( "moreConfiguration" )
                                                                            .setHeight( 600 )
                                                                            .setWidth( 800 )
                                                                            .addIgnore( MetricName.BOX_PLOT_OF_ERRORS )
                                                                            .setLeadUnit( DurationUnit.MINUTES )
                                                                            .setShape( GraphicShape.LEAD_THRESHOLD )
                                                                            .setTemplateName( "anotherTemplate" ) ) )
                                .setProtobuf( ProtobufFormat.newBuilder() )
                                .setNetcdf( NetcdfFormat.newBuilder() )
                                .build();

        Outputs first = zeroth.toBuilder()
                              .build();

        assertEquals( zeroth, zeroth );
        assertEquals( zeroth, first );
        assertEquals( 0, MessageUtilities.compare( zeroth, zeroth ) );
        assertEquals( 0, MessageUtilities.compare( zeroth, first ) );
        // Less than and greater than examples
        Outputs second = Outputs.newBuilder()
                                .setCsv( CsvFormat.newBuilder()
                                                  .setOptions( NumericFormat.newBuilder()
                                                                            .setDecimalFormat( "0.0" ) ) )
                                .build();

        assertTrue( MessageUtilities.compare( first, second ) > 0 );

        Outputs third = Outputs.newBuilder()
                               .setCsv( CsvFormat.newBuilder()
                                                 .setOptions( NumericFormat.newBuilder()
                                                                           .setDecimalFormat( "0.00" ) ) )
                               .build();

        assertTrue( MessageUtilities.compare( second, third ) < 0 );

        Outputs fourth = Outputs.newBuilder()
                                .setPng( PngFormat.newBuilder()
                                                  .setOptions( GraphicFormat.newBuilder()
                                                                            .setConfiguration( "someConfiguration" )
                                                                            .setHeight( 600 )
                                                                            .setWidth( 800 )
                                                                            .addIgnore( MetricName.BIAS_FRACTION )
                                                                            .setLeadUnit( DurationUnit.HOURS )
                                                                            .setShape( GraphicShape.ISSUED_DATE_POOLS )
                                                                            .setTemplateName( "aTemplate" ) ) )
                                .build();

        Outputs fifth = Outputs.newBuilder()
                               .setPng( PngFormat.newBuilder()
                                                 .setOptions( GraphicFormat.newBuilder()
                                                                           .setConfiguration( "someConfiguration" )
                                                                           .setHeight( 600 )
                                                                           .setWidth( 800 )
                                                                           .addIgnore( MetricName.BIAS_FRACTION )
                                                                           .setLeadUnit( DurationUnit.DAYS )
                                                                           .setShape( GraphicShape.ISSUED_DATE_POOLS )
                                                                           .setTemplateName( "aTemplate" ) ) )
                               .build();

        assertTrue( MessageUtilities.compare( fourth, fifth ) < 0 );

        Outputs sixth = Outputs.newBuilder()
                               .setProtobuf( ProtobufFormat.newBuilder() )
                               .setNetcdf( NetcdfFormat.newBuilder() )
                               .build();

        assertTrue( MessageUtilities.compare( Outputs.getDefaultInstance(), sixth ) < 0 );
    }

    @Test
    void testCompareSeason()
    {
        // Consistent with equals

        // Compare default instances
        assertEquals( 0, MessageUtilities.compare( Season.getDefaultInstance(), Season.getDefaultInstance() ) );
        // Compare full instance
        Season first = Season.newBuilder()
                             .setStartDay( 1 )
                             .setEndDay( 1 )
                             .setStartMonth( 1 )
                             .setEndMonth( 12 )
                             .build();

        Season second = first.toBuilder()
                             .build();

        assertEquals( first, first );
        assertEquals( first, second );
        assertEquals( 0, MessageUtilities.compare( first, first ) );
        assertEquals( 0, MessageUtilities.compare( first, second ) );

        // Less than and greater than examples
        Season third = Season.newBuilder()
                             .setStartDay( 1 )
                             .setEndDay( 1 )
                             .build();

        assertTrue( MessageUtilities.compare( third, first ) < 0 );

        // Less than and greater than examples
        Season fourth = Season.newBuilder()
                              .setStartDay( 2 )
                              .setEndDay( 1 )
                              .build();

        assertTrue( MessageUtilities.compare( third, fourth ) < 0 );
    }

    @Test
    void testCompareValueFilter()
    {
        // Consistent with equals

        // Compare default instances
        assertEquals( 0,
                      MessageUtilities.compare( ValueFilter.getDefaultInstance(),
                                                ValueFilter.getDefaultInstance() ) );
        // Compare full instance
        ValueFilter first = ValueFilter.newBuilder()
                                       .setMinimumInclusiveValue( 0.0 )
                                       .setMaximumInclusiveValue( 1.0 )
                                       .build();

        ValueFilter second = first.toBuilder()
                                  .build();

        assertEquals( first, first );
        assertEquals( first, second );
        assertEquals( 0, MessageUtilities.compare( first, first ) );
        assertEquals( 0, MessageUtilities.compare( first, second ) );

        // Less than and greater than examples
        ValueFilter third = ValueFilter.newBuilder()
                                       .setMinimumInclusiveValue( 0.0 )
                                       .build();

        assertTrue( MessageUtilities.compare( third, first ) < 0 );

        ValueFilter fourth = ValueFilter.newBuilder()
                                        .setMinimumInclusiveValue( 0.0 )
                                        .setMaximumInclusiveValue( 2.0 )
                                        .build();

        assertTrue( MessageUtilities.compare( fourth, first ) > 0 );

        ValueFilter fifth = ValueFilter.newBuilder()
                                       .setMinimumInclusiveValue( 1.0 )
                                       .setMaximumInclusiveValue( 2.0 )
                                       .build();

        assertTrue( MessageUtilities.compare( fourth, fifth ) < 0 );
    }

    @Test
    void testComparePool()
    {
        // Consistent with equals

        // Compare default instances
        assertEquals( 0, MessageUtilities.compare( Pool.getDefaultInstance(), Pool.getDefaultInstance() ) );
        // Compare full instance
        Pool first = Pool.newBuilder()
                         .addGeometryTuples( GeometryTuple.newBuilder()
                                                          .setLeft( Geometry.newBuilder()
                                                                            .setName( "left" )
                                                                            .setDescription( "description" )
                                                                            .setSrid( 5643 )
                                                                            .setWkt( "POINT( 1 2 )" ) )
                                                          .setRight( Geometry.newBuilder()
                                                                             .setName( "right" )
                                                                             .setDescription( "description" )
                                                                             .setSrid( 5643 )
                                                                             .setWkt( "POINT( 1 2 )" ) ) )
                         .setIsBaselinePool( false )
                         .setEventThreshold( Threshold.newBuilder()
                                                      .setLeftThresholdValue( DoubleValue.newBuilder()
                                                                                         .setValue( 1.0 ) )
                                                      .setRightThresholdValue( DoubleValue.newBuilder()
                                                                                          .setValue( 1.0 ) ) )
                         .setDecisionThreshold( Threshold.newBuilder()
                                                         .setLeftThresholdValue( DoubleValue.newBuilder()
                                                                                            .setValue( 2.0 ) )
                                                         .setRightThresholdValue( DoubleValue.newBuilder()
                                                                                             .setValue( 2.0 ) ) )
                         .setTimeScale( TimeScale.newBuilder()
                                                 .setFunction( TimeScale.TimeScaleFunction.MAXIMUM )
                                                 .setPeriod( Duration.newBuilder()
                                                                     .setSeconds( 12345 ) ) )
                         .setTimeWindow( TimeWindow.newBuilder()
                                                   .setEarliestLeadDuration( Duration.newBuilder()
                                                                                     .setSeconds( 56789 ) )
                                                   .setLatestLeadDuration( Duration.newBuilder()
                                                                                   .setSeconds( 89101112 ) ) )
                         .setPairs( Pairs.newBuilder()
                                         .addTimeSeries( TimeSeriesOfPairs.newBuilder()
                                                                          .addPairs( Pair.newBuilder()
                                                                                         .addLeft( 1.0 )
                                                                                         .addRight( 2.0 ) ) ) )
                         .build();

        Pool second = first.toBuilder()
                           .build();

        assertEquals( first, first );
        assertEquals( first, second );
        assertEquals( 0, MessageUtilities.compare( first, first ) );
        assertEquals( 0, MessageUtilities.compare( first, second ) );

        // Less than and greater than examples
        Pool third = Pool.newBuilder()
                         .addGeometryTuples( GeometryTuple.newBuilder()
                                                          .setLeft( Geometry.newBuilder()
                                                                            .setName( "left" )
                                                                            .setDescription( "description" )
                                                                            .setSrid( 5643 )
                                                                            .setWkt( "POINT( 1 2 )" ) ) )
                         .build();

        Pool fourth = Pool.newBuilder()
                          .addGeometryTuples( GeometryTuple.newBuilder()
                                                           .setLeft( Geometry.newBuilder()
                                                                             .setName( "left" )
                                                                             .setDescription( "description" )
                                                                             .setSrid( 5643 )
                                                                             .setWkt( "POINT( 3 4 )" ) ) )
                          .build();

        assertTrue( MessageUtilities.compare( third, fourth ) < 0 );
        assertTrue( MessageUtilities.compare( third, first ) < 0 );

        Pool fifth = Pool.newBuilder()
                         .setIsBaselinePool( false )
                         .build();

        Pool sixth = Pool.newBuilder()
                         .setIsBaselinePool( true )
                         .build();

        assertTrue( MessageUtilities.compare( fifth, sixth ) < 0 );

        Pool seventh = Pool.newBuilder()
                           .setEventThreshold( Threshold.newBuilder()
                                                        .setLeftThresholdValue( DoubleValue.newBuilder()
                                                                                           .setValue( 1.0 ) )
                                                        .setRightThresholdValue( DoubleValue.newBuilder()
                                                                                            .setValue( 1.0 ) ) )
                           .build();

        Pool eighth = Pool.newBuilder()
                          .setEventThreshold( Threshold.newBuilder()
                                                       .setLeftThresholdValue( DoubleValue.newBuilder()
                                                                                          .setValue( 2.0 ) )
                                                       .setRightThresholdValue( DoubleValue.newBuilder()
                                                                                           .setValue( 2.0 ) ) )
                          .build();

        assertTrue( MessageUtilities.compare( seventh, eighth ) < 0 );

        Pool ninth = Pool.newBuilder()
                         .setDecisionThreshold( Threshold.newBuilder()
                                                         .setLeftThresholdValue( DoubleValue.newBuilder()
                                                                                            .setValue( 1.0 ) )
                                                         .setRightThresholdValue( DoubleValue.newBuilder()
                                                                                             .setValue( 1.0 ) ) )
                         .build();

        Pool tenth = Pool.newBuilder()
                         .setDecisionThreshold( Threshold.newBuilder()
                                                         .setLeftThresholdValue( DoubleValue.newBuilder()
                                                                                            .setValue( 2.0 ) )
                                                         .setRightThresholdValue( DoubleValue.newBuilder()
                                                                                             .setValue( 2.0 ) ) )
                         .build();

        assertTrue( MessageUtilities.compare( ninth, tenth ) < 0 );

        Pool eleventh = Pool.newBuilder()
                            .setTimeScale( TimeScale.newBuilder()
                                                    .setFunction( TimeScale.TimeScaleFunction.MAXIMUM )
                                                    .setPeriod( Duration.newBuilder()
                                                                        .setSeconds( 01234 ) ) )
                            .build();

        Pool twelfth = Pool.newBuilder()
                           .setTimeScale( TimeScale.newBuilder()
                                                   .setFunction( TimeScale.TimeScaleFunction.MAXIMUM )
                                                   .setPeriod( Duration.newBuilder()
                                                                       .setSeconds( 12345 ) ) )
                           .build();

        assertTrue( MessageUtilities.compare( eleventh, twelfth ) < 0 );

        Pool thirteenth = Pool.newBuilder()
                              .setTimeWindow( TimeWindow.newBuilder()
                                                        .setEarliestLeadDuration( Duration.newBuilder()
                                                                                          .setSeconds( 45678 ) ) )
                              .build();

        Pool fourteenth = Pool.newBuilder()
                              .setTimeWindow( TimeWindow.newBuilder()
                                                        .setEarliestLeadDuration( Duration.newBuilder()
                                                                                          .setSeconds( 56789 ) ) )
                              .build();

        assertTrue( MessageUtilities.compare( thirteenth, fourteenth ) < 0 );

        Pool fifteenth = Pool.newBuilder()
                             .setPairs( Pairs.newBuilder()
                                             .addTimeSeries( TimeSeriesOfPairs.newBuilder()
                                                                              .addPairs( Pair.newBuilder()
                                                                                             .addLeft( 1.0 )
                                                                                             .addRight( 2.0 ) )
                                                                              .addReferenceTimes( ReferenceTime.newBuilder()
                                                                                                               .setReferenceTimeType( ReferenceTimeType.ANALYSIS_START_TIME )
                                                                                                               .setReferenceTime( Timestamp.newBuilder()
                                                                                                                                           .setSeconds( 1234 ) ) ) ) )
                             .build();

        Pool sixteenth = Pool.newBuilder()
                             .setPairs( Pairs.newBuilder()
                                             .addTimeSeries( TimeSeriesOfPairs.newBuilder()
                                                                              .addPairs( Pair.newBuilder()
                                                                                             .addLeft( 3.0 )
                                                                                             .addRight( 2.0 ) )
                                                                              .addReferenceTimes( ReferenceTime.newBuilder()
                                                                                                               .setReferenceTimeType( ReferenceTimeType.ANALYSIS_START_TIME )
                                                                                                               .setReferenceTime( Timestamp.newBuilder()
                                                                                                                                           .setSeconds( 1234 ) ) ) ) )
                             .build();

        assertTrue( MessageUtilities.compare( fifteenth, sixteenth ) < 0 );
    }
}
