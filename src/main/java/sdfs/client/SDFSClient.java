/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.namenode.SDFSFileChannel;
import sdfs.protocol.IDataNodeProtocol;
import sdfs.protocol.INameNodeProtocol;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;

public class SDFSClient implements ISimpleDistributedFileSystem {
    private INameNodeProtocol nameNode;
    private int fileDataBlockCacheSize;
    /**
     * @param fileDataBlockCacheSize Buffer size for file data block. By default, it should be 16.
     *                               That means 16 block of data will be cache on local.
     *                               And you should use LRU algorithm to replace it.
     *                               It may be change during test. So don't assert it will equal to a constant.
     */
    public SDFSClient(InetSocketAddress inetSocketAddress, int fileDataBlockCacheSize) {
        nameNode = new NameNodeStub(inetSocketAddress);
        this.fileDataBlockCacheSize = fileDataBlockCacheSize;
    }

    @Override
    public SDFSFileChannel openReadonly(String fileUri) throws IOException {

        SDFSFileChannel fileChannel = nameNode.openReadonly(fileUri);
        if (fileChannel==null)
            throw new FileNotFoundException();
        fileChannel.config(nameNode,fileDataBlockCacheSize);
        return fileChannel;
    }

    @Override
    public SDFSFileChannel create(String fileUri) throws IOException {
        SDFSFileChannel fileChannel = nameNode.create(fileUri);
        if (fileChannel==null)
            throw new FileNotFoundException();

        fileChannel.config(nameNode,fileDataBlockCacheSize);
        return fileChannel;
    }

    @Override
    public SDFSFileChannel openReadWrite(String fileUri) throws IOException {
        SDFSFileChannel fileChannel = nameNode.openReadwrite(fileUri);
        if (fileChannel==null)
            throw new FileNotFoundException();

        fileChannel.config(nameNode,fileDataBlockCacheSize);
        return fileChannel;
    }
    @Override
    public void delete(String fileUri) throws Exception {
        nameNode.delete(fileUri);
    }

    @Override
    public void mkdir(String fileUri) throws IOException {
        nameNode.mkdir(fileUri);
    }
}
