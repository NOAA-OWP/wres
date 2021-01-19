package wres.vis;

import org.apache.commons.lang3.tuple.Pair;
import org.jfree.data.xy.AbstractXYDataset;

import wres.datamodel.FeatureKey;
import wres.datamodel.FeatureTuple;
import wres.datamodel.sampledata.SampleData;

/**
 * An {@link AbstractXYDataset} for single-valued pairs.
 * 
 * @author Hank.Herr
 */
//TODO Note that this needs further work whenever we let wres-vis build scatter plots for display.
//Specifically, we need to think about what the legend entry should look like (see below for a first attempt)
//as well as how to handle multiple sereis being plotted (i.e., how to store the data.).
class SingleValuedPairsXYDataset
        extends WRESAbstractXYDataset<SampleData<Pair<Double, Double>>, SampleData<Pair<Double, Double>>> //implements DomainInfo, XisSymbolic, RangeInfo
{
    private static final long serialVersionUID = 4183134607345060330L;

    SingleValuedPairsXYDataset( final SampleData<Pair<Double, Double>> input )
    {
        super( input );
    }

    @Override
    protected void preparePlotData( final SampleData<Pair<Double, Double>> rawData )
    {
        setPlotData( rawData );
    }

    @Override
    public int getItemCount( final int series )
    {
        return getPlotData().getRawData().size();
    }

    @Override
    public Number getX( final int series, final int item )
    {
        return getPlotData().getRawData().get( item ).getLeft();
    }

    @Override
    public Number getY( final int series, final int item )
    {
        return getPlotData().getRawData().get( item ).getRight();
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
                              .getGeometryTuplesList()
                              .get( 0 )
                              .getLeft()
                              .getName();

        FeatureKey leftKey = FeatureKey.of( leftName );

        String rightName = this.getPlotData()
                               .getMetadata()
                               .getPool()
                               .getGeometryTuplesList()
                               .get( 0 )
                               .getRight()
                               .getName();

        FeatureKey rightKey = FeatureKey.of( rightName );

        FeatureKey baselineKey = null;
        
        boolean hasBaseline = this.getPlotData()
                .getMetadata()
                .getPool()
                .getGeometryTuplesList()
                .get( 0 )
                .hasBaseline();
        
        if( hasBaseline )
        {
            String baselineName = this.getPlotData()
                    .getMetadata()
                    .getPool()
                    .getGeometryTuplesList()
                    .get( 0 )
                    .getBaseline()
                    .getName();
            
            baselineKey = FeatureKey.of( baselineName );
        }
        
        FeatureTuple tuple = new FeatureTuple( leftKey, rightKey, baselineKey );
        
        return tuple + "."
               + getPlotData().getMetadata().getEvaluation().getRightVariableName()
               + "."
               + getPlotData().getMetadata().getEvaluation().getRightDataName();
    }
}
