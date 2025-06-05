package wres.vis.charts;

import java.awt.*;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.statistics.generated.MetricName;

/**
 * Tests the {@link GraphicsUtils}.
 *
 * @author James Brown
 */
class GraphicsUtilsTest
{
    @Test
    void testGetColors()
    {
        Color[] colors = GraphicsUtils.getColors();

        // No yellow colors
        assertAll( () -> assertTrue( colors.length > 0 ),
                   () -> assertTrue( Arrays.stream( colors )
                                           .noneMatch( c -> c.getRed() == 255
                                                            && c.getGreen() == 255 ) ) );

    }

    @Test
    void testIsNotDefaultMetricComponentName()
    {
        assertAll( () -> assertTrue( GraphicsUtils.isNotDefaultMetricComponentName( MetricName.MEAN ) ),
                   () -> assertFalse( GraphicsUtils.isNotDefaultMetricComponentName( MetricName.UNDEFINED ) ),
                   () -> assertFalse( GraphicsUtils.isNotDefaultMetricComponentName( MetricName.MAIN ) ) );
    }

    @Test
    void testGetColorPalette()
    {
        Color[] colors = GraphicsUtils.getColorPalette( 10, Color.GREEN, Color.RED );

        // No yellow colors
        assertAll( () -> assertEquals( 10, colors.length ),
                   () -> assertTrue( Arrays.asList( colors )
                                           .contains( Color.RED ) ),
                   () -> assertTrue( Arrays.asList( colors )
                                           .contains( Color.GREEN ) ) );

    }
}