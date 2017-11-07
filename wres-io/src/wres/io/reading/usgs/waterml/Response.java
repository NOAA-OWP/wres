package wres.io.reading.usgs.waterml;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Response
{
    String name;

    String declaredType;

    String scope;

    ResponseValue value;

    Boolean nil;

    public String getName()
    {
        return name;
    }

    public void setName( String name )
    {
        this.name = name;
    }

    public String getDeclaredType()
    {
        return declaredType;
    }

    public void setDeclaredType( String declaredType )
    {
        this.declaredType = declaredType;
    }

    public String getScope()
    {
        return scope;
    }

    public void setScope( String scope )
    {
        this.scope = scope;
    }

    public ResponseValue getValue()
    {
        return value;
    }

    public void setValue( ResponseValue value )
    {
        this.value = value;
    }

    public Boolean getNil()
    {
        return nil;
    }

    public void setNil( Boolean nil )
    {
        this.nil = nil;
    }

    public Boolean getGlobalScope()
    {
        return globalScope;
    }

    public void setGlobalScope( Boolean globalScope )
    {
        this.globalScope = globalScope;
    }

    public Boolean getTypeSubstituted()
    {
        return typeSubstituted;
    }

    public void setTypeSubstituted( Boolean typeSubstituted )
    {
        this.typeSubstituted = typeSubstituted;
    }

    Boolean globalScope;

    Boolean typeSubstituted;
}
