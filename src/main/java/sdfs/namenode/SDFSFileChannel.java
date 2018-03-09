/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.namenode;

import sdfs.client.DataNodeStub;
import sdfs.datanode.DataNodeServer;
import sdfs.filetree.BlockInfo;
import sdfs.filetree.FileNode;
import sdfs.protocol.IDataNodeProtocol;
import sdfs.protocol.INameNodeProtocol;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.*;

public class SDFSFileChannel implements SeekableByteChannel, Flushable, Serializable {
    private static final long serialVersionUID = 6892411224902751501L;
    private static final int BLOCK_SIZE = DataNodeServer.BLOCK_SIZE;
    private INameNodeProtocol nameNode;
    private final UUID uuid; //File uuid
    private long fileSize; //Size of this file
    private int blockAmount; //Total block amount of this file
    public final FileNode fileNode;
    public final boolean isReadOnly;
    private boolean closed;
    private long ptr;
    private int cacheSize;
    private final Map<LocatedBlock, byte[]> dataBlocksCache = new LinkedHashMap<>(16, 0.1f, true);
    //BlockNumber to DataBlock cache. byte[] or ByteBuffer are both acceptable.

    SDFSFileChannel(UUID uuid, int fileSize, int blockAmount, FileNode fileNode, boolean isReadOnly) {
        this.uuid = uuid;
        this.fileSize = fileSize;
        this.blockAmount = blockAmount;
        this.fileNode = fileNode;
        this.isReadOnly = isReadOnly;
        nameNode = null;
        closed = false;
        ptr = 0;
    }

    public void config(INameNodeProtocol nameNode,int cacheSize){
        this.nameNode = nameNode;
        this.cacheSize = cacheSize;
    }
    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (closed)
            throw new ClosedChannelException();
        if (ptr >= fileSize)
            return 0;
        byte[] tmp = new byte[BLOCK_SIZE];
        int len = dst.remaining();
        int readCount = 0;
        Iterator<BlockInfo> blockInfo = fileNode.iterator();
        long position = 0;
        while (readCount<len&&blockInfo.hasNext()){

            BlockInfo blk = blockInfo.next();
            position += BLOCK_SIZE;
            if (position <= ptr)
                continue;
            int c = read(blk.iterator().next(),ptr-position+BLOCK_SIZE,Math.min(len-readCount,position-ptr),tmp);
            if (c < 0)
                return readCount;
            dst.put(tmp,0,c);
            readCount += c;
            ptr+=c;
        }

        return readCount;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if (closed)
            throw new ClosedChannelException();
        if (isReadOnly)
            throw new NonWritableChannelException();
        if (ptr>fileSize){
            int amount = (int) (ptr/BLOCK_SIZE + (ptr%BLOCK_SIZE==0?0:1));
            if (amount>blockAmount)
                addBlocks(amount-blockAmount);
            fileSize = ptr;
        }
        int len = src.remaining();
        int writeCount = 0;

        byte[] buff = new byte[BLOCK_SIZE];
        Iterator<BlockInfo>  blockInfo = fileNode.iterator();
        long position = 0;
        while (writeCount<len){
            while (!blockInfo.hasNext()){
                addBlocks(1);
                blockInfo = fileNode.iterator();
                position = 0;
            }
            BlockInfo blk = blockInfo.next();
            position += BLOCK_SIZE;
            if (position <= ptr)
                continue;
            int wLen = (int) Math.min(position-ptr,len-writeCount);
            src.get(buff,0,wLen);
            wLen = write(blk.iterator().next(),ptr-position+BLOCK_SIZE,wLen,buff);
            writeCount+=wLen;
            ptr+=wLen;
            fileSize = Math.max(ptr,fileSize);
        }
        return writeCount;
    }

    private void addBlocks(int n) throws IOException {
        for (int i=0;i<n;i++){
            LocatedBlock lk = nameNode.addBlock(uuid);
            BlockInfo blk = new BlockInfo();
            blk.addLocatedBlock(lk);
            fileNode.addBlockInfo(blk);
            put(lk,new byte[BLOCK_SIZE]);
        }
        blockAmount = fileNode.getBlockAmount();
    }
    private void removeBlocks(int n) throws IOException {
        nameNode.removeLastBlocks(uuid,n);
        for (int i=0;i<n;i++){
            LocatedBlock lk = fileNode.last();
            fileNode.removeLastBlockInfo();
            put(lk,new byte[BLOCK_SIZE]);
        }
        blockAmount = fileNode.getBlockAmount();
    }
    @Override
    public long position() throws IOException {
        if (closed)
            throw new ClosedChannelException();
        return ptr;
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        if (closed)
            throw new ClosedChannelException();

        if (newPosition < 0)
            throw new IllegalArgumentException();
        ptr = newPosition;
        return this;
    }

    @Override
    public long size() throws IOException {
        if (closed)
            throw new ClosedChannelException();
        return fileSize;
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        if (closed)
            throw new ClosedChannelException();
        if (isReadOnly)
            throw new NonWritableChannelException();
        if (size<0)
            throw new IllegalArgumentException();
        if (size >= fileSize){
            if (position()>size)
                ptr = size;
            return this;
        }else {
            int amount = (int) (size/BLOCK_SIZE + (size%BLOCK_SIZE==0?0:1));
            if (amount<blockAmount)
                removeBlocks(blockAmount-amount);
            fileSize = size;
            if (position()>size)
                ptr = size;
            LocatedBlock last = fileNode.last();
            if (last!=null){
                byte[] newLast = new byte[BLOCK_SIZE];
                read(fileNode.last(),0,BLOCK_SIZE,newLast);
                put(fileNode.last(),newLast);
            }
            return this;
        }
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    @Override
    public void close() throws IOException {
        if (closed)
            throw new ClosedChannelException();
        if (!isReadOnly){
            flush();
            dataBlocksCache.clear();
            nameNode.closeReadwriteFile(uuid, (int) fileSize);
        }else
            nameNode.closeReadonlyFile(uuid);
        closed = true;
    }
    private void load(LocatedBlock lk) throws IOException{
        if (dataBlocksCache.containsKey(lk))
            return;
        IDataNodeProtocol dataNode = new DataNodeStub(lk.getInetAddress());
        byte[] bytes;
        try {
            bytes = dataNode.read(uuid,lk.getBlockNumber(),0,BLOCK_SIZE);
        }catch (FileNotFoundException e){
            bytes = new byte[BLOCK_SIZE];
        }

        put(lk,bytes);
    }
    private void toHost(LocatedBlock lk,byte[] bytes) throws IOException {
        byte[] tmp = bytes;
        if (lk.equals(fileNode.last())) {
            int len = (int) (fileSize - BLOCK_SIZE * (blockAmount - 1));
            tmp = new byte[len];
            System.arraycopy(bytes,0,tmp,0,len);
        }
        IDataNodeProtocol dataNode = new DataNodeStub(lk.getInetAddress());
        dataNode.write(uuid,lk.getBlockNumber(),0,tmp);
    }
    @Override
    public void flush() throws IOException {
        if (closed)
            throw new ClosedChannelException();
        if (isReadOnly)
            throw new NonWritableChannelException();
        Set<Map.Entry<LocatedBlock,byte[]>> caches = dataBlocksCache.entrySet();
        for (Map.Entry<LocatedBlock,byte[]> e:caches
                )
            toHost(e.getKey(),e.getValue());
    }
    private void put(LocatedBlock id, byte[] b) throws IOException {
        if (id == null)
            return;
        if (b==null || b.length==0)
            put(id,new byte[BLOCK_SIZE]);
        dataBlocksCache.remove(id);
        if (b.length!=BLOCK_SIZE){
            byte[] tmp = new byte[BLOCK_SIZE];
            System.arraycopy(b,0,tmp,0,b.length);
            b = tmp;
        }
        if (dataBlocksCache.size()>=cacheSize) {
            LocatedBlock lk = dataBlocksCache.keySet().iterator().next();
            byte[] bytes = dataBlocksCache.remove(lk);
            if(!isReadOnly)
                toHost(lk,bytes);
        }
        dataBlocksCache.put(id,b);
    }
    private int read(LocatedBlock id, long offset, long size, byte[] b) throws IndexOutOfBoundsException, IOException {
        if (id == null)
            return -1;
        load(id);
        byte[] block = dataBlocksCache.get(id);
        int len = BLOCK_SIZE;
        if (id.equals(fileNode.last()))
            len = (int) (fileSize-BLOCK_SIZE*(blockAmount-1));
        int numRead = (int) Math.min(len-offset,size);
        if (numRead==0)
            return -1;
        if ( offset>=0 && size<=b.length && numRead>0)
            System.arraycopy(block, (int) offset,b,0,numRead);
        else
            throw new IndexOutOfBoundsException();
        return numRead;
    }
    private int write(LocatedBlock id, long offset, long size, byte[] b) throws IndexOutOfBoundsException, IOException {
        if (id == null)
            return -1;
        load(id);
        byte[] block = dataBlocksCache.get(id);

        int numWrite = (int) Math.min(BLOCK_SIZE-offset,size);
        if (numWrite == 0)
            return -1;
        if ( offset>=0 && size<=b.length &&  numWrite > 0)
            System.arraycopy(b,0,block,(int)offset,numWrite);
        else
            throw new IndexOutOfBoundsException();
        return numWrite;

    }
}
