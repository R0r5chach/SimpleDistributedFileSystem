/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.filetree;

import sdfs.namenode.LocatedBlock;
import sdfs.namenode.NameNodeServer;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FileNode extends Node implements Serializable, Iterable<BlockInfo>, Cloneable {
    private static final long serialVersionUID = -5007570814999866661L;
    private List<BlockInfo> blockInfos = new ArrayList<>();

    private int fileSize;//file size should be checked when closing the file.
//    public FileNode(){
//    }
    public FileNode(int id) {
        super(id, TYPE.FILE);
        fileSize = 0;
    }

    @Override
    void init(String wd) throws IOException {
        if (inited)
            return;
        blockInfos.clear();
        File file = new File(wd + id + ".node");
        FileInputStream fis = new FileInputStream(file);
        ObjectInputStream oi = new ObjectInputStream(fis);
        try {
            fileSize = oi.readInt();
            BlockInfo bl;
            while ( (bl=(BlockInfo)oi.readObject())!=null){
                addBlockInfo(bl);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            fis.close();
            oi.close();
        }
        inited = true;
    }
    @Override
    public void toDisk() {
        File block = new File(NameNodeServer.wd + id + ".node");
        FileOutputStream fo;
        try {
            fo = new FileOutputStream(block);
            ObjectOutputStream oop = new ObjectOutputStream(fo);
            oop.writeInt(fileSize);
            for (BlockInfo bl:blockInfos
                    ) {
                oop.writeObject(bl);
            }
            oop.writeObject(null);
            oop.close();
            fo.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public LocatedBlock last(){
        if (getBlockAmount() == 0)
            return null;
        return blockInfos.get(blockInfos.size()-1).iterator().next();
    }

    public int getBlockAmount(){return blockInfos.size();}
    public void addBlockInfo(BlockInfo blockInfo) {
        blockInfos.add(blockInfo);
    }

    public void removeLastBlockInfo() {
        blockInfos.remove(blockInfos.size() - 1);
    }

    public int getFileSize() {
        return fileSize;
    }

    public void setFileSize(int fileSize) {
        this.fileSize = fileSize;
    }
    public void clear(){
        fileSize = 0;
        blockInfos.clear();
    }
    @Override
    public Iterator<BlockInfo> iterator() {
        return blockInfos.listIterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FileNode that = (FileNode) o;

        return blockInfos.equals(that.blockInfos);
    }

    @Override
    public int hashCode() {
        return blockInfos.hashCode();
    }
    public Object clone(){
        FileNode o = null;
        try {

            o = (FileNode) super.clone();
            o.blockInfos = new ArrayList<>(blockInfos);

        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return  o;
    }
}

