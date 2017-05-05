/**
 * 
 */
package data;

import java.util.ArrayList;
import java.util.LinkedList;

import collections.Pair;
import collections.RealCollection;

/**
 * @author Christopher Tubbs
 *
 */
public class ValuePairs extends LinkedList<Pair<Double, RealCollection>>
{    
    public void add(Float value, RealCollection collection)
    {
        this.add(new Pair<Double, RealCollection>(value * 1.0, collection));
    }
    
    public void add(Double value, RealCollection collection)
    {
        this.add(new Pair<Double, RealCollection>(value, collection));
    }
    
    public void add(float value, RealCollection collection)
    {
        this.add(new Pair<Double, RealCollection>(value * 1.0, collection));
    }
    
    public void add(double value, RealCollection collection)
    {
        this.add(new Pair<Double, RealCollection>(value, collection));
    }
    
    public void add(Integer value, RealCollection collection)
    {
        this.add(new Pair<Double, RealCollection>(value * 1.0, collection));
    }
    
    public void add(int value, RealCollection collection)
    {
        this.add(new Pair<Double, RealCollection>(value * 1.0, collection));
    }
    
    public void add(short value, RealCollection collection)
    {
        this.add(new Pair<Double, RealCollection>(value * 1.0, collection));
    }
    
    public void add(Short value, RealCollection collection)
    {
        this.add(new Pair<Double, RealCollection>(value * 1.0, collection));
    }
    
    public void add(Long value, RealCollection collection)
    {
        this.add(new Pair<Double, RealCollection>(value * 1.0, collection));
    }
    
    public void add(long value, RealCollection collection)
    {
        this.add(new Pair<Double, RealCollection>(value * 1.0, collection));
    }
    
    public void add(Float observedValue, Float[] forecasts)
    {
        this.add(observedValue, new RealCollection(forecasts));
    }
    
    public void add(Float observedValue, Double[] forecasts)
    {
        this.add(observedValue, new RealCollection(forecasts));
    }
    
    public void add(Float observedValue, float[] forecasts)
    {
        this.add(observedValue, new RealCollection(forecasts));
    }
    
    public void add(Float observedValue, double[] forecasts)
    {
        this.add(observedValue, new RealCollection(forecasts));
    }
    
    public void add(Float observedValue, double forecast)
    {
        this.add(observedValue, new RealCollection(forecast));
    }
    
    public void add(Float observedValue, Double forecast)
    {
        this.add(observedValue, new RealCollection(forecast));
    }
    
    public void add(Double observedValue, Float[] forecasts)
    {
        this.add(observedValue, new RealCollection(forecasts));
    }
    
    public void add(Double observedValue, Double[] forecasts)
    {
        this.add(observedValue, new RealCollection(forecasts));
    }
    
    public void add(Double observedValue, float[] forecasts)
    {
        this.add(observedValue, new RealCollection(forecasts));
    }
    
    public void add(Double observedValue, double[] forecasts)
    {
        this.add(observedValue, new RealCollection(forecasts));
    }
    
    public void add(Double observedValue, double forecast)
    {
        this.add(observedValue, new RealCollection(forecast));
    }
    
    public void add(Double observedValue, Double forecast)
    {
        this.add(observedValue, new RealCollection(forecast));
    }
    
    
}
