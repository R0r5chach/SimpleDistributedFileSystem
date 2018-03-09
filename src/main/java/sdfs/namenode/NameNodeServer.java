/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.namenode;
import sdfs.exception.SDFSFileAlreadyExistException;
import sdfs.filetree.*;
import sdfs.protocol.INameNodeDataNodeProtocol;
import sdfs.protocol.INameNodeProtocol;
import sun.misc.UUDecoder;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.*;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.FileAlreadyExistsException;
import java.util.*;

import static sdfs.filetree.Node.TYPE.DIR;

public class NameNodeServer implements INameNodeProtocol, INameNodeDataNodeProtocol {
    public static final int DATA_NODE_PORT = 4341;

    private static final int BLOCK_SIZE = 128 * 1024;
    private final Map<UUID, FileNode> readonlyFile = new HashMap<>();
    private final Map<UUID, FileNode> readwriteFile = new HashMap<>();
    private final Map<UUID, FileNode> rwCopy = new HashMap<>();
    private Map<String,BitSet> dataNodes = new HashMap<>();
    public int nameNodePort;
    private int ID = 0;
    private DirNode rootNode;
    public final static String wd = "files/namenode/";
    public NameNodeServer(int nameNodePort) throws IOException {
        this.nameNodePort = nameNodePort;
        rootNode = new DirNode(ID);
        File dir = new File(wd);
        if (!dir.exists())
            dir.mkdirs();

        File root = new File(wd + "0.node");
        File metaData = new File(wd+"metadata");
        // FileOutputStream fi = new FileOutputStream(root);
        if (!root.exists()){
            boolean result = root.createNewFile();
            rootNode.toDisk();
        }
        ID++;
        if (metaData.exists()){
            try {
                ObjectInputStream fis = new ObjectInputStream(new FileInputStream(metaData));
                ID = fis.readInt();
                dataNodes = (HashMap<String, BitSet>) fis.readObject();
                fis.close();
            } catch (IOException ignored) {
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }else {
            dataNodes = new HashMap<>();
            BitSet bitSet = new BitSet();
            dataNodes.put("localhost",bitSet);
            toDisk();
        }
        rootNode.init(wd);
    }
    private void toDisk(){
        File metaData = new File(wd+"metadata");
        try {
            FileOutputStream fos = new FileOutputStream(metaData);
            ObjectOutputStream dos = new ObjectOutputStream(fos);
            dos.writeInt(ID);
            dos.writeObject(dataNodes);
            fos.close();
            dos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private Node lookUp(String fileUri, Node.TYPE type) throws URISyntaxException, IOException {

        DirNode dir = rootNode;

        String tmp = fileUri;
        if ("/".equals(fileUri))
            return rootNode;
        if (tmp.startsWith("/")){
            tmp = tmp.substring(1,tmp.length());
            dir = rootNode;
        }
        if (tmp.endsWith("/")){
            tmp = tmp.substring(0,tmp.length()-1);
        }
        int ptr;

        while((ptr=tmp.indexOf("/"))!=-1){
            dir = (DirNode) dir.findEntry(tmp.substring(0,ptr), DIR);
            if (dir==null)
                throw new URISyntaxException(fileUri.substring(0,fileUri.lastIndexOf("/")),"no such file");
            tmp = tmp.substring(ptr+1);
        }
        return dir.findEntry(tmp, type);
    }
    @Override
    public SDFSFileChannel openReadonly(String fileUri) throws IOException {
        FileNode entity;
        try {
            entity = (FileNode) lookUp(fileUri, Node.TYPE.FILE);
        } catch (URISyntaxException e) {
            throw new FileNotFoundException();
        }
        if (entity==null)
            throw new FileNotFoundException();
        UUID uuid = UUID.randomUUID();
        if (readwriteFile.containsValue(entity)){
            UUID uuid1 = getKey(readwriteFile,entity);
            FileNode fileNode = rwCopy.get(uuid1);
            SDFSFileChannel fileChannel = new SDFSFileChannel(uuid,fileNode.getFileSize(),fileNode.getBlockAmount(),fileNode,true);
            readonlyFile.put(uuid,fileNode);
            return fileChannel;
        }
        SDFSFileChannel fileChannel = new SDFSFileChannel(uuid,entity.getFileSize(),entity.getBlockAmount(),entity,true);
        readonlyFile.put(uuid,entity);
        return fileChannel;
    }

    @Override
    public SDFSFileChannel openReadwrite(String fileUri) throws IndexOutOfBoundsException, IllegalStateException, IOException {
        FileNode entity;
        try {
            entity = (FileNode) lookUp(fileUri, Node.TYPE.FILE);
        } catch (URISyntaxException e) {
            throw new FileNotFoundException();
        }
        if (entity==null)
            throw new FileNotFoundException();
        if (readwriteFile.containsValue(entity))
            throw new OverlappingFileLockException();

        UUID uuid = UUID.randomUUID();
        SDFSFileChannel fileChannel = new SDFSFileChannel(uuid,entity.getFileSize(),entity.getBlockAmount(),entity,false);
        readwriteFile.put(uuid,entity);
        rwCopy.put(uuid, (FileNode) entity.clone());
        return fileChannel;
    }

    @Override
    public SDFSFileChannel create(String fileUri) throws IOException {
        if (fileUri.startsWith("/")){
            fileUri = fileUri.substring(1,fileUri.length());
        }
        DirNode dir= rootNode;
        Node e = null;
        try {
            e = lookUp(fileUri, Node.TYPE.FILE);
        }catch (URISyntaxException exception){
            throw new FileNotFoundException();
        }
        if (e!=null)
            throw new SDFSFileAlreadyExistException();
        try {
            e = lookUp(fileUri, Node.TYPE.DIR);
        }catch (URISyntaxException exception){
            mkdir(exception.getInput());
        }
        if (e!=null)
            throw new SDFSFileAlreadyExistException();
        FileNode fileNode;
        int p;
        if ((p = fileUri.lastIndexOf("/"))!=-1){
            try {
                dir= (DirNode) lookUp(fileUri.substring(0,p), Node.TYPE.DIR);
            } catch (URISyntaxException e1) {
                e1.printStackTrace();
            }
        }
        if (p==fileUri.length()-1)
            throw new IOException();
        else {
            fileNode = new FileNode(ID);
            Entry entry = new Entry(fileUri.substring(p+1,fileUri.length()),fileNode);
            ID++;
            toDisk();
            fileNode.inited = true;
            dir.addEntry(entry,true);
            //fileNode.incRef();

        }
        //
        UUID uuid = UUID.randomUUID();
        SDFSFileChannel fileChannel = new SDFSFileChannel(uuid,fileNode.getFileSize(),fileNode.getBlockAmount(),fileNode,false);
        readwriteFile.put(uuid,fileNode);
        rwCopy.put(uuid, (FileNode) fileNode.clone());
        return fileChannel;
    }

    @Override
    public void closeReadonlyFile(UUID fileUuid) throws IllegalStateException, IOException {
        if (!readonlyFile.containsKey(fileUuid))
            throw new IllegalStateException();
        readonlyFile.remove(fileUuid);
    }

    @Override
    public void closeReadwriteFile(UUID fileUuid, int newFileSize) throws IllegalStateException, IllegalArgumentException, IOException {
        if (!readwriteFile.containsKey(fileUuid))
            throw new IllegalStateException();
        FileNode fileNode = readwriteFile.get(fileUuid);
        int n = fileNode.getBlockAmount();
        if (!((n-1) * BLOCK_SIZE<newFileSize && newFileSize<=n * BLOCK_SIZE))
            throw new IllegalArgumentException();
        else
            fileNode.setFileSize(newFileSize);
        readwriteFile.remove(fileUuid);
        rwCopy.remove(fileUuid);
        fileNode.toDisk();
    }

    @Override
    public void mkdir(String fileUri) throws IOException {
        if (fileUri.startsWith("/")){
            fileUri = fileUri.substring(1,fileUri.length());
        }
        if (fileUri.endsWith("/")){
            fileUri = fileUri.substring(0,fileUri.length()-1);
        }
        Node e = null;
        try {
            e = lookUp(fileUri, Node.TYPE.DIR);
        }catch (URISyntaxException exception){

            throw new FileNotFoundException();
        }

        if (e!=null)
            throw new SDFSFileAlreadyExistException();
        try {
            e = lookUp(fileUri, Node.TYPE.FILE);
        } catch (URISyntaxException e1) {
            e1.printStackTrace();
        }
        if (e!=null)
            throw new SDFSFileAlreadyExistException();
        DirNode dir= rootNode;
        int p;
        if ((p = fileUri.lastIndexOf("/"))!=-1){
            try {
                dir= (DirNode) lookUp(fileUri.substring(0,p), Node.TYPE.DIR);
            } catch (URISyntaxException e1) {
                e1.printStackTrace();
            }
        }
        if (p==fileUri.length()-1)
            throw new IOException();
        else {
            DirNode dirNode = new DirNode(ID);
            Entry entry = new Entry(fileUri.substring(p+1,fileUri.length()),dirNode);
            ID++;
            toDisk();
            dir.addEntry(entry,true);
            dirNode.inited = true;
            dirNode.toDisk();
        }
    }

    @Override
    public LocatedBlock addBlock(UUID fileUuid) {
        if (!readwriteFile.containsKey(fileUuid))
            throw new IllegalStateException();
        FileNode fileNode = readwriteFile.get(fileUuid);

        BitSet bitSet = dataNodes.get("localhost");
        int blockID = -1;
        for (int i = 0;i < bitSet.size();i++){
            if (!bitSet.get(i)){
                blockID = i;
                break;
            }
        }
        if (blockID == -1)
            blockID = bitSet.size();
        bitSet.set(blockID);
        try {
            LocatedBlock lk = new LocatedBlock(InetAddress.getByName("localhost"),blockID);
            BlockInfo blockInfo = new BlockInfo();
            blockInfo.addLocatedBlock(lk);
            fileNode.addBlockInfo(blockInfo);
            return lk;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<LocatedBlock> addBlocks(UUID fileUuid, int blockAmount) {
        List<LocatedBlock> locatedBlocks = new LinkedList<>();
        for (int i=0;i<blockAmount;i++){
            locatedBlocks.add(addBlock(fileUuid));
        }
        return locatedBlocks;
    }

    @Override
    public void removeLastBlock(UUID fileUuid) throws IllegalStateException {
        if (!readwriteFile.containsKey(fileUuid))
            throw new IllegalStateException();
        FileNode fileNode = readwriteFile.get(fileUuid);
        if (fileNode.getBlockAmount()==0)
            throw new IllegalStateException();
        LocatedBlock lk = fileNode.last();
        BitSet bitSet = dataNodes.get("localhost");
        if (lk.getInetAddress().getHostName().equals("localhost"))
            bitSet.clear(lk.getBlockNumber());
        fileNode.removeLastBlockInfo();
    }

    @Override
    public void removeLastBlocks(UUID fileUuid, int blockAmount) throws IllegalStateException {
        for (int i=0;i<blockAmount;i++){
            removeLastBlock(fileUuid);
        }
    }

    @Override
    public void delete(String fileUri) throws IllegalStateException, IOException {
        if ("/".equals(fileUri))
            throw new IOException("root is not allowed to delete");
        if (fileUri.startsWith("/")){
            fileUri = fileUri.substring(1,fileUri.length());
        }
        if (fileUri.endsWith("/")){
            fileUri = fileUri.substring(0,fileUri.length()-1);
        }
        DirNode dir= rootNode;
        int p;
        if ((p = fileUri.lastIndexOf("/"))!=-1){
            try {
                dir= (DirNode) lookUp(fileUri.substring(0,p), Node.TYPE.DIR);
            } catch (URISyntaxException e1) {
                e1.printStackTrace();
            }
        }
        DirNode dirNode = null;
        try {
            dirNode = (DirNode) lookUp(fileUri, Node.TYPE.DIR);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        if (dirNode!=null){
            dirNode.clear();
            dir.rmEntity(dirNode.id,Node.TYPE.DIR);
            File file = new File(wd+dirNode.id+".node");
            if (file.exists())
                file.delete();
            return;
        }
        FileNode entity = null;
        try {
            entity = (FileNode) lookUp(fileUri, Node.TYPE.FILE);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        if (entity==null)
            throw new FileNotFoundException("no such file or dir");
        if (readonlyFile.containsValue(entity)||readwriteFile.containsValue(entity))
            throw new IllegalStateException("someone else is reading or writing this file!");
        for (BlockInfo bl:entity
             ) {
            for (LocatedBlock lk:bl
                 ) {
                BitSet bitSet = dataNodes.get("localhost");
                if (lk.getInetAddress().getHostName().equals("localhost"))
                    bitSet.clear(lk.getBlockNumber());
            }
        }
        entity.clear();
        File file = new File(wd+entity.id+".node");
        if (file.exists())
            file.delete();
        dir.rmEntity(entity.id, Node.TYPE.FILE);
    }

    //根据map的value获取map的key
    private static UUID getKey(Map<UUID,FileNode> map,FileNode value){
        UUID key = null;
        for (Map.Entry<UUID,FileNode> entry : map.entrySet()) {
            if(value.equals(entry.getValue())){
                key=entry.getKey();
            }
        }
        return key;
    }

}

class NameNodeController extends Thread{
    private static INameNodeProtocol nameNode;
    private Socket client;
    private NameNodeController(Socket socket) {
        this.client = socket;
    }
    /*
     * 线程执行方法
     */
    public void run() {
        try {
            //System.out.println("accept invoke");

            ObjectInputStream in = new ObjectInputStream(client.getInputStream());
            ObjectOutputStream out = new ObjectOutputStream(client.getOutputStream());
            if (!"NameNodeServer".equals(in.readUTF()))
                return;
            String methodName = in.readUTF();
            Class<?>[] paraTypes = (Class<?>[]) in.readObject();
            Object[] parameters = (Object[]) in.readObject();
            Method method = nameNode.getClass().getMethod(methodName,paraTypes);
            Object result;
            try{
                result = method.invoke(nameNode,parameters);
                out.writeObject(result);
            }catch(InvocationTargetException e) {
                out.writeObject(e.getTargetException());
            }finally {
                in.close();
                out.close();
                client.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void main(String args[]){

        ServerSocket server;
        try {
            nameNode = new NameNodeServer(4340);
            server = new ServerSocket(4340);
            while (true){
                Socket socket = server.accept();
                NameNodeController nc = new NameNodeController(socket);
                nc.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}