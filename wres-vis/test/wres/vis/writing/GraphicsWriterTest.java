package wres.vis.writing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.time.temporal.ChronoUnit;

import org.junit.Test;

import wres.statistics.generated.Outputs;
import wres.statistics.generated.Outputs.GraphicFormat;
import wres.statistics.generated.Outputs.PngFormat;
import wres.statistics.generated.Outputs.SvgFormat;
import wres.statistics.generated.Outputs.GraphicFormat.DurationUnit;
import wres.vis.writing.GraphicsWriter.GraphicsHelper;

/**
 * Tests the {@link GraphicsWriter}.
 */

public class GraphicsWriterTest
{

    @Test
    public void testGetDurationUnitsFromOutputs()
    {
        Outputs outputs = Outputs.getDefaultInstance();
        GraphicsHelper helper = GraphicsHelper.of( outputs );
        
        ChronoUnit actual = helper.getDurationUnits();

        assertEquals( ChronoUnit.HOURS, actual );

        Outputs anotherOutputs =
                Outputs.newBuilder()
                       .setPng( PngFormat.newBuilder()
                                         .setOptions( GraphicFormat.newBuilder().setLeadUnit( DurationUnit.MINUTES ) ) )
                       .build();

        GraphicsHelper anotherHelper = GraphicsHelper.of( anotherOutputs );
        ChronoUnit anotherActual = anotherHelper.getDurationUnits();

        assertEquals( ChronoUnit.MINUTES, anotherActual );

        Outputs yetAnotherOutputs =
                Outputs.newBuilder()
                       .setSvg( SvgFormat.newBuilder()
                                         .setOptions( GraphicFormat.newBuilder().setLeadUnit( DurationUnit.SECONDS ) ) )
                       .build();

        GraphicsHelper yetAnotherHelper = GraphicsHelper.of( yetAnotherOutputs );
        ChronoUnit yetAnotherActual = yetAnotherHelper.getDurationUnits();

        assertEquals( ChronoUnit.SECONDS, yetAnotherActual );

        Outputs oneMoreOutputs =
                Outputs.newBuilder()
                       .setSvg( SvgFormat.newBuilder()
                                         .setOptions( GraphicFormat.newBuilder().setLeadUnit( DurationUnit.DAYS ) ) )
                       .setPng( PngFormat.newBuilder()
                                         .setOptions( GraphicFormat.newBuilder().setLeadUnit( DurationUnit.DAYS ) ) )
                       .build();

        GraphicsHelper oneMoreHelper = GraphicsHelper.of( oneMoreOutputs );
        ChronoUnit oneMoreActual = oneMoreHelper.getDurationUnits();

        assertEquals( ChronoUnit.DAYS, oneMoreActual );
    }

    @Test
    public void testGetDurationUnitsFromOutputsThrowsExpectedExceptionWhenMultipleUnitsArePresent()
    {
        Outputs outputs =
                Outputs.newBuilder()
                       .setSvg( SvgFormat.newBuilder()
                                         .setOptions( GraphicFormat.newBuilder().setLeadUnit( DurationUnit.MINUTES ) ) )
                       .setPng( PngFormat.newBuilder()
                                         .setOptions( GraphicFormat.newBuilder().setLeadUnit( DurationUnit.DAYS ) ) )
                       .build();
        
        assertThrows( IllegalArgumentException.class, () -> GraphicsHelper.of( outputs ) );
    }


}
