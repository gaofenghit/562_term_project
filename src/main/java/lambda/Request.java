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
    String bucketname;
    String filename;
    int row;
    int col;
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
    public int getRow() {
        return row;
    }
    public void setRow(int v) {
        row = v;
    }
    public int getCol() {
        return col;
    }
    public void setCol(int v) {
        col = v;
    }
    public Request()
    {

    }
}
