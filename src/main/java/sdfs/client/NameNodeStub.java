/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.namenode.LocatedBlock;
import sdfs.namenode.SDFSFileChannel;
import sdfs.protocol.INameNodeProtocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;

public class NameNodeStub implements INameNodeProtocol {
    private InetSocketAddress inetSocketAddress;
    NameNodeStub(InetSocketAddress inetSocketAddress){
        this.inetSocketAddress = inetSocketAddress;
    }
    @Override
    public SDFSFileChannel openReadonly(String fileUri) throws IOException {
        Class<?>[] paraTypes = {String.class};
        Object[] parameters = {fileUri};
        Object b = Stub.rpc(Stub.Server.NAMENODE,"openReadonly",paraTypes,parameters,inetSocketAddress);
        if (b instanceof IOException)
            throw (IOException) b;
        if (b instanceof SDFSFileChannel)
            return (SDFSFileChannel) b;
        else
            throw new IOException();
    }

    @Override
    public SDFSFileChannel openReadwrite(String fileUri) throws IndexOutOfBoundsException, IllegalStateException, IOException {
        Class<?>[] paraTypes = {String.class};
        Object[] parameters = {fileUri};

        Object b = Stub.rpc(Stub.Server.NAMENODE,"openReadwrite",paraTypes,parameters,inetSocketAddress);
        if (b instanceof IndexOutOfBoundsException)
            throw (IndexOutOfBoundsException) b;
        if (b instanceof IllegalStateException)
            throw (IllegalStateException) b;
        if (b instanceof IOException)
            throw (IOException)b;
        if (b instanceof SDFSFileChannel)
            return (SDFSFileChannel) b;
        else
            throw new IOException();
    }

    @Override
    public SDFSFileChannel create(String fileUri) throws IllegalStateException, IOException {
        Class<?>[] paraTypes = {String.class};
        Object[] parameters = {fileUri};
        Object b = Stub.rpc(Stub.Server.NAMENODE,"create",paraTypes,parameters,inetSocketAddress);
        if (b instanceof IllegalStateException)
            throw (IllegalStateException) b;
        if (b instanceof IOException)
            throw (IOException)b;
        if (b instanceof SDFSFileChannel)
            return (SDFSFileChannel) b;
        else
            throw new IOException();
    }

    @Override
    public void closeReadonlyFile(UUID fileUuid) throws IllegalStateException, IOException {
        Class<?>[] paraTypes = {UUID.class};
        Object[] parameters = {fileUuid};
        Object b = Stub.rpc(Stub.Server.NAMENODE,"closeReadonlyFile",paraTypes,parameters,inetSocketAddress);
        if (b instanceof IllegalStateException)
            throw (IllegalStateException) b;
        if (b instanceof IOException)
            throw (IOException)b;
        if (b != null) {
            throw new IOException();
        }
    }

    @Override
    public void closeReadwriteFile(UUID fileUuid, int newFileSize) throws IllegalStateException, IllegalArgumentException, IOException {
        Class<?>[] paraTypes = {UUID.class,int.class};
        Object[] parameters = {fileUuid,newFileSize};
        Object b = Stub.rpc(Stub.Server.NAMENODE,"closeReadwriteFile",paraTypes,parameters,inetSocketAddress);
        if (b instanceof IllegalStateException)
            throw (IllegalStateException) b;
        if (b instanceof IllegalArgumentException)
            throw (IllegalArgumentException)b;
        if (b instanceof IOException)
            throw (IOException)b;
        if (b != null) {
            throw new IOException();
        }
    }

    @Override
    public void mkdir(String fileUri) throws IOException {
        Class<?>[] paraTypes = {String.class};
        Object[] parameters = {fileUri};
        Object b = Stub.rpc(Stub.Server.NAMENODE,"mkdir",paraTypes,parameters,inetSocketAddress);
        if (b instanceof IOException)
            throw (IOException)b;
        if (b != null) {
            throw new IOException();
        }
    }

    @Override
    public LocatedBlock addBlock(UUID fileUuid) {
        Class<?>[] paraTypes = {UUID.class};
        Object[] parameters = {fileUuid};
        Object b = null;
        try {
            b = Stub.rpc(Stub.Server.NAMENODE,"addBlock",paraTypes,parameters,inetSocketAddress);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (b instanceof IllegalStateException)
            throw (IllegalStateException) b;
        return (LocatedBlock) b;
    }

    @Override
    public List<LocatedBlock> addBlocks(UUID fileUuid, int blockAmount) {
        Class<?>[] paraTypes = {UUID.class, int.class};
        Object[] parameters = {fileUuid, blockAmount};
        Object b = null;
        try {
            b = Stub.rpc(Stub.Server.NAMENODE,"addBlocks",paraTypes,parameters,inetSocketAddress);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (b instanceof IllegalStateException)
            throw (IllegalStateException) b;
        return (List<LocatedBlock>) b;
    }

    @Override
    public void removeLastBlock(UUID fileUuid) throws IllegalStateException {
        Class<?>[] paraTypes = {UUID.class};
        Object[] parameters = {fileUuid};
        Object b = null;
        try {
            b = Stub.rpc(Stub.Server.NAMENODE,"removeLastBlock",paraTypes,parameters,inetSocketAddress);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (b instanceof IllegalStateException)
            throw (IllegalStateException) b;
    }

    @Override
    public void removeLastBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException {
        Class<?>[] paraTypes = {UUID.class, int.class};
        Object[] parameters = {fileUuid,blockAmount};
        Object b = null;
        try {
            b = Stub.rpc(Stub.Server.NAMENODE,"removeLastBlocks",paraTypes,parameters,inetSocketAddress);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (b instanceof IllegalStateException)
            throw (IllegalStateException) b;
    }

    @Override
    public void delete(String fileUri) throws IllegalStateException, IOException{
        Class<?>[] paraTypes = {String.class};
        Object[] parameters = {fileUri};
        Object b = Stub.rpc(Stub.Server.NAMENODE,"delete",paraTypes,parameters,inetSocketAddress);
        if (b instanceof IllegalStateException)
            throw (IllegalStateException) b;
        if (b instanceof IOException)
            throw (IOException)b;
        if (b != null) {
            throw new IOException();
        }
    }
}
