/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lambda;

/**
 *
 * @author wlloyd
 */
public class Request {
    String name;
    String bucketname;
    String filename;
    String aggregation;
    String filter;
    String flag;
    
    public String getName()
    {
        return name;
    }
    public void setName(String name)
    {
        this.name = name;
    }
    public Request(String name)
    {
        this.name = name;
    }
    public String getBucketname()
    {
        return bucketname;
    }
    public void setBucketname(String name)
    {
        this.bucketname = name;
    }
    public String getFilename()
    {
        return filename;
    }
    public void setFilename(String name)
    {
        this.filename = name;
    }
    public String getAggregation()
    {
        return aggregation;
    }
    public void setAggregation(String q)
    {
        this.aggregation = q;
    }
    public String getFilter()
    {
        return filter;
    }
    public void setFilter(String q)
    {
        this.filter = q;
    }
    public String getFlag()
    {
        return flag;
    }
    public void setflag(String q)
    {
        this.flag = q;
    }
    
    public Request()
    {
        
    }
}
