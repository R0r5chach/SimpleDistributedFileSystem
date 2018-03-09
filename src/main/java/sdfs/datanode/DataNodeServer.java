/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.datanode;

import sdfs.protocol.IDataNodeProtocol;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.UUID;

public class DataNodeServer implements IDataNodeProtocol {
    /**
     * The block size may be changed during test.
     * So please use this constant.
     */

    public static final int BLOCK_SIZE = 128 * 1024;
    public static final int DATA_NODE_PORT = 4341;
    //    put off due to its difficulties
    //    private final Map<UUID, Set<Integer>> uuidReadonlyPermissionCache = new HashMap<>();
    //    private final Map<UUID, Set<Integer>> uuidReadwritePermissionCache = new HashMap<>();
    //working dir
    private static final String wd =  "files/datanode0/";
    private int blockID;
    private ArrayList<Integer> freeList;

    private final InetAddress inetAddress;
    //在构造函数里进行一些初始化操作（读取关于空闲块的信息）
    DataNodeServer(String address) throws IOException {
        this.inetAddress = InetAddress.getByName(address);
        blockID = 0;
        freeList = new ArrayList<>();
        File dir = new File(wd);
        if (!dir.exists())
            dir.mkdirs();
        File metaData = new File(wd+"metadata");
        if (metaData.exists()){
            try {
                DataInputStream fis = new DataInputStream(new FileInputStream(metaData));
                blockID = fis.readInt();
                int len = fis.readInt();
                for (int i = 0;i<len;i++){
                    freeList.add(fis.readInt());
                }
            } catch (IOException ignored) {
            }
        }
    }

    //保存blockID, freeList等关于空闲块的信息到磁盘
    private void toDisk(){
        File metaData = new File(wd+"metadata");
        try {
            FileOutputStream fos = new FileOutputStream(metaData);
            DataOutputStream dos = new DataOutputStream(fos);
            dos.writeInt(blockID);
            dos.writeInt(freeList.size());
            for (Integer i:freeList
                    ) {
                dos.writeInt(i);
            }
            fos.close();
            dos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //返回一个空闲块号到NameNode
    public int allocBlock() {
        int num;
        if (freeList.isEmpty()){
            num = blockID;
            blockID++;
        }else {
            num = freeList.get(0);
            freeList.remove(0);
        }
        toDisk();
        return num;
    }
    /*
     * Read data from a block.
     * redirect to [blockNumber].block file
     */
    @Override
    public byte[] read(UUID fileUuid, int blockNumber, int offset, int size) throws IndexOutOfBoundsException, IOException {
        File block = new File( wd + blockNumber + ".block");
        if (!block.exists())
            throw new FileNotFoundException("block "+ blockNumber+" not exists");

        RandomAccessFile fi = new RandomAccessFile(wd + blockNumber + ".block","r");
        long len = block.length();
        byte[] b;
        byte[] data;
        //FileInputStream fi = new FileInputStream(block);
        int numRead;

        if (offset<=len && offset>=0  && size>=0){
            b = new byte[size];
            fi.seek(offset);
            numRead = fi.read(b,0,size);
            data = new byte[numRead];
            System.arraycopy(b,0,data,0,numRead);
        }else {
            throw new IndexOutOfBoundsException();
        }
        fi.close();
        return data;
    }
    /*
         * Write data to a block.
         * redirect to [blockNumber].block file
         */
    @Override
    public void write(UUID fileUuid, int blockNumber, int offset, byte[] b) throws IndexOutOfBoundsException, IOException {
        File block = new File( wd + blockNumber + ".block");
        RandomAccessFile fo = new RandomAccessFile(block,"rw");
        fo.seek(offset);
        int size = b.length;
        if (offset+size<=BLOCK_SIZE && offset>=0)
            fo.write(b,0,size);
        else
            throw new IndexOutOfBoundsException();
        fo.close();
    }

//    @Override
//    //删除对应的block文件，将对应块号加入空闲块列表中
//    public void delete(int blockNumber) throws IOException {
//        File block = new File( wd + blockNumber + ".block");
//        if (block.exists())
//            block.delete();
//        if (blockID == blockNumber+1 )
//            blockID = blockNumber;
//        else
//            freeList.add(blockNumber);
//        toDisk();
//    }
}
class DataNodeController extends Thread{
    private static IDataNodeProtocol dataNode;
    private Socket client;
    private DataNodeController(Socket socket) {
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
            if (!"DataNodeServer".equals(in.readUTF()))
                return;
            String methodName = in.readUTF();
            Class<?>[] paraTypes = (Class<?>[]) in.readObject();
            Object[] parameters = (Object[]) in.readObject();
            Method method = dataNode.getClass().getMethod(methodName,paraTypes);
            Object result;
            try{
                result = method.invoke(dataNode,parameters);
                out.writeObject(result);

            }catch(InvocationTargetException e) {
                out.writeObject(e.getCause());
            }finally {
                out.flush();
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
            dataNode = new DataNodeServer("localhost");
            server = new ServerSocket(DataNodeServer.DATA_NODE_PORT);
            while (true){
                Socket socket = server.accept();
                DataNodeController dc = new DataNodeController(socket);
                dc.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}