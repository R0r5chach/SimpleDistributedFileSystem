/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.filetree;

import java.io.IOException;
import java.io.Serializable;

public abstract class Node implements Serializable,Cloneable{
    private static final long serialVersionUID = -3286564461647015367L;
    public enum TYPE{
        FILE,DIR
    }
    public int id;
    TYPE type;
    transient public boolean inited;
    //Node(){}
    Node(int id,TYPE type){
        this.id = id;
        this.type = type;
    }
    void toDisk(){}
    void init(String wd) throws IOException{}
}