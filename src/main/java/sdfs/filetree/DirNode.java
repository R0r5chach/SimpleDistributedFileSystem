/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.filetree;

import sdfs.namenode.NameNodeServer;

import java.io.*;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class DirNode extends Node implements Serializable, Iterable<Entry> {
    private static final long serialVersionUID = 8178778592344231767L;
    transient private Set<Entry> entries = new HashSet<>();
    public DirNode(int id){
        super(id, TYPE.DIR);
    }
    //与toDisk对应，从磁盘上恢复DirNode
    public void init(String wd) throws IOException {
        if (inited)
            return;
        if (entries == null)
            entries = new HashSet<>();

        File file = new File(wd + id + ".node");
        FileInputStream fis = new FileInputStream(file);
        ObjectInputStream oi = new ObjectInputStream(fis);
        try {
            Entry entry;
            while ( (entry=(Entry) oi.readObject())!=null)
                addEntry(entry,false);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            fis.close();
            oi.close();
        }
        inited = true;
    }
    @Override
    public Iterator<Entry> iterator() {
        return entries.iterator();
    }

    public boolean addEntry(Entry entry, boolean flag) {
        boolean result = entries.add(entry);
        if (flag)
            this.toDisk();
        return result;
    }
    public Node findEntry(String name,TYPE type) throws IOException {
        for (Entry e:entries
                ) {
            if (e.hashCode()==name.hashCode()){
                if (e.getNode().type!=type)
                    return null;
                Node node =  e.getNode();
                if (!node.inited)
                    node.init(NameNodeServer.wd);
                return node;
            }
        }
        return null;
    }
    public void clear() throws IOException {
        if (entries==null)
            return;
        if (!entries.isEmpty())
            throw new IOException("Not empty dir is not allowed to delete!");
    }
    //将关于DirNode的信息存储到在磁盘上
    public void toDisk(){
        File file = new File(NameNodeServer.wd + id + ".node");
        FileOutputStream fo;
        try {
            fo = new FileOutputStream(file);
            ObjectOutputStream oop = new ObjectOutputStream(fo);
            for (Entry e:entries
                        ) {
                    oop.writeObject(e);
            }
            oop.writeObject(null);
            oop.close();
            fo.close();
        }  catch (IOException e) {
            e.printStackTrace();
        }

    }
    //删除子文件/子文件夹
    public void rmEntity(int id,TYPE type){
        Entry tmp = null;
        for (Entry e:entries
                ) {
            if (e.getNode().id==id&&e.getNode().type==type)
                tmp = e;
        }
        if (tmp != null)
            entries.remove(tmp);
        this.toDisk();
    }
    public boolean removeEntry(Entry entry) {
        return entries.remove(entry);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DirNode entries1 = (DirNode) o;

        return entries.equals(entries1.entries);
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }
}
