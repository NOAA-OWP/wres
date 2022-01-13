package wres.vis;

import org.apache.commons.lang3.tuple.Pair;
import org.jfree.data.xy.AbstractXYDataset;

import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.Pool;
import wres.datamodel.space.FeatureTuple;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;

/**
 * An {@link AbstractXYDataset} for single-valued pairs.
 * 
 * @author Hank.Herr
 */
//TODO Note that this needs further work whenever we let wres-vis build scatter plots for display.
//Specifically, we need to think about what the legend entry should look like (see below for a first attempt)
//as well as how to handle multiple sereis being plotted (i.e., how to store the data.).
class SingleValuedPairsXYDataset
        extends WRESAbstractXYDataset<Pool<Pair<Double, Double>>, Pool<Pair<Double, Double>>> //implements DomainInfo, XisSymbolic, RangeInfo
{
    private static final long serialVersionUID = 4183134607345060330L;

    SingleValuedPairsXYDataset( final Pool<Pair<Double, Double>> input )
    {
        super( input );
    }

    @Override
    protected void preparePlotData( final Pool<Pair<Double, Double>> rawData )
    {
        setPlotData( rawData );
    }

    @Override
    public int getItemCount( final int series )
    {
        return getPlotData().get().size();
    }

    @Override
    public Number getX( final int series, final int item )
    {
        return getPlotData().get().get( item ).getLeft();
    }

    @Override
    public Number getY( final int series, final int item )
    {
        return getPlotData().get().get( item ).getRight();
    }

    @Override
    public int getSeriesCount()
    {
        return 1;
    }

    @Override
    public Comparable<String> getSeriesKey( final int series )
    {
        if ( isLegendNameOverridden( series ) )
        {
            return getOverrideLegendName( series );
        }

        String leftName = this.getPlotData()
                              .getMetadata()
                              .getPool()
                              .getGeometryGroup()
                              .getGeometryTuplesList()
                              .get( 0 )
                              .getLeft()
                              .getName();

        Geometry left = MessageFactory.getGeometry( leftName );

        String rightName = this.getPlotData()
                               .getMetadata()
                               .getPool()
                               .getGeometryGroup()
                               .getGeometryTuplesList()
                               .get( 0 )
                               .getRight()
                               .getName();

        Geometry right = MessageFactory.getGeometry( rightName );

        Geometry baseline = null;
        
        boolean hasBaseline = this.getPlotData()
                .getMetadata()
                .getPool()
                .getGeometryGroup()
                .getGeometryTuplesList()
                .get( 0 )
                .hasBaseline();
        
        if( hasBaseline )
        {
            String baselineName = this.getPlotData()
                    .getMetadata()
                    .getPool()
                    .getGeometryGroup()
                    .getGeometryTuplesList()
                    .get( 0 )
                    .getBaseline()
                    .getName();
            
            baseline = MessageFactory.getGeometry( baselineName );
        }
        
        GeometryTuple geometryTuple = MessageFactory.getGeometryTuple( left, right, baseline );
        FeatureTuple tuple = FeatureTuple.of( geometryTuple );
        
        return tuple + "."
               + getPlotData().getMetadata().getEvaluation().getRightVariableName()
               + "."
               + getPlotData().getMetadata().getEvaluation().getRightDataName();
    }
}
