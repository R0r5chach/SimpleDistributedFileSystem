/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.datanode;

import sdfs.protocol.IDataNodeProtocol;

import java.io.IOException;
import java.util.UUID;

public class DataNode implements IDataNodeProtocol {
    /**
     * The block size may be changed during test.
     * So please use this constant.
     */
    public static final int BLOCK_SIZE = 128 * 1024;
    public static final int DATA_NODE_PORT = 4341;
    //    put off due to its difficulties
    //    private final Map<UUID, Set<Integer>> uuidReadonlyPermissionCache = new HashMap<>();
    //    private final Map<UUID, Set<Integer>> uuidReadwritePermissionCache = new HashMap<>();

    @Override
    public byte[] read(UUID fileUuid, int blockNumber, int offset, int size) throws IndexOutOfBoundsException, IOException {
        return new byte[0];
    }

    @Override
    public void write(UUID fileUuid, int blockNumber, int offset, byte[] b) throws IndexOutOfBoundsException, IOException {

    }
}
