/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.namenode;

import sdfs.filetree.FileNode;
import sdfs.protocol.INameNodeDataNodeProtocol;
import sdfs.protocol.INameNodeProtocol;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class NameNode implements INameNodeProtocol, INameNodeDataNodeProtocol {
    public static final int NAME_NODE_PORT = 4341;
    private final Map<UUID, FileNode> readonlyFile = new HashMap<>();
    private final Map<UUID, FileNode> readwritePFile = new HashMap<>();

    public NameNode(int nameNodePort) {

    }

    @Override
    public SDFSFileChannel openReadonly(String fileUri) throws IOException {
        return null;
    }

    @Override
    public SDFSFileChannel openReadwrite(String fileUri) throws IndexOutOfBoundsException, IllegalStateException, IOException {
        return null;
    }

    @Override
    public SDFSFileChannel create(String fileUri) throws IOException {
        return null;
    }

    @Override
    public void closeReadonlyFile(UUID fileUuid) throws IllegalStateException, IOException {

    }

    @Override
    public void closeReadwriteFile(UUID fileUuid, int newFileSize) throws IllegalStateException, IllegalArgumentException, IOException {

    }

    @Override
    public void mkdir(String fileUri) throws IOException {

    }

    @Override
    public LocatedBlock addBlock(UUID fileUuid) {
        return null;
    }

    @Override
    public List<LocatedBlock> addBlocks(UUID fileUuid, int blockAmount) {
        return null;
    }

    @Override
    public void removeLastBlock(UUID fileUuid) throws IllegalStateException {

    }

    @Override
    public void removeLastBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException {

    }
}
