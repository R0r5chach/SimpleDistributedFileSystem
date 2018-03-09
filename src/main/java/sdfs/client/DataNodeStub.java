/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.protocol.IDataNodeProtocol;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.UUID;

public class DataNodeStub implements IDataNodeProtocol {
    private InetSocketAddress inetSocketAddress;

    private static final int DATA_NODE_PORT = 4341;
    public DataNodeStub(InetAddress inetAddress) {
        this.inetSocketAddress = new InetSocketAddress(inetAddress.getHostName(),DATA_NODE_PORT);
    }

    @Override
    public byte[] read(UUID fileUuid, int blockNumber, int offset, int size) throws IndexOutOfBoundsException, IOException {
        Class<?>[] paraTypes = {UUID.class, int.class, int.class, int.class};
        Object[] parameters = {fileUuid,blockNumber,offset,size};
        Object b = Stub.rpc(Stub.Server.DATANODE,"read",paraTypes,parameters,inetSocketAddress);
        if (b instanceof IOException)
            throw (IOException) b;
        if (b instanceof IndexOutOfBoundsException)
            throw (IndexOutOfBoundsException)b;
        if (b instanceof byte[])
            return (byte[]) b;
        else
            throw new IOException();
    }

    @Override
    public void write(UUID fileUuid, int blockNumber, int offset, byte[] b) throws IndexOutOfBoundsException, IOException {
        Class<?>[] paraTypes = {UUID.class, int.class, int.class, byte[].class};
        Object[] parameters = {fileUuid,blockNumber,offset,b};
        Object result = Stub.rpc(Stub.Server.DATANODE,"write",paraTypes,parameters,inetSocketAddress);
        if (result instanceof IOException)
            throw (IOException) result;
        if (result instanceof IndexOutOfBoundsException)
            throw (IndexOutOfBoundsException)result;
        if (result != null) {
            throw new IOException();
        }
    }

}

