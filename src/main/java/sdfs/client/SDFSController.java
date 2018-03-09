/*
* Created by xk on 2017/10/12 16.
*/

package sdfs.client;
import sdfs.namenode.SDFSFileChannel;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SDFSController {
    public static void main(String[] args){
        try {
            SDFSClient simpleDistributedSystem = new SDFSClient(new InetSocketAddress("localhost",4340),12);
            String usage = "wrong command\n" +
                    "put: put [local file path] [sdfs://ip[:port]/path]\n" +
                    "get: get [local file path] [sdfs://ip[:port]/path]\n" +
                    "delete: delete [sdfs://ip[:port]/path]\n" +
                    "mkdir: mkdir [sdfs://ip[:port]/path]\n" +
                    "ls: ls [sdfs://ip[:port]/path]\n";
            if (args.length<1)
                System.out.println(usage);
            String s = args[0];
            if ("put".equals(s)){
                try {
                    if (args.length!=3){
                        System.out.println(usage);
                        System.exit(0);
                    }

                    String uri = getUri(args[2]);
                    File input = new File(args[1]);

                    FileChannel fi = new FileInputStream(input).getChannel();

                    SDFSFileChannel fileChannel = simpleDistributedSystem.create(uri);
                    ByteBuffer byteBuffer = ByteBuffer.allocate(100000);
                    while (fi.read(byteBuffer) >0){
                        byteBuffer.flip();
                        fileChannel.write(byteBuffer);
                        byteBuffer.flip();
                    }
                    fileChannel.close();
                }catch (Exception e) {
                    e.printStackTrace();
                }
            } else if ("get".equals(s)){
                try {
                    if (args.length!=3){
                        System.out.println(usage);
                        System.exit(0);
                    }
                    String uri = getUri(args[2]);
                   // String uri = args[2].substring(args[2].indexOf("/",args[2].lastIndexOf(":")),args[2].length());
                    String fileName = args[1];
                    int p;
                    if ((p = fileName.lastIndexOf("/"))!=-1){
                        try {
                            fileName = fileName.substring(0,p);
                            File dir = new File(fileName);
                            if (!dir.exists())
                                dir.mkdirs();
                        } catch (Exception e1) {
                            e1.printStackTrace();
                        }
                    }
                    SDFSFileChannel fileChannel = simpleDistributedSystem.openReadonly(uri);
                    File output = new File(args[1]);
                    FileChannel fo = new FileOutputStream(output).getChannel();
                    ByteBuffer byteBuffer = ByteBuffer.allocate(10000);

                    while (fileChannel.read(byteBuffer)>0){
                        byteBuffer.flip();
                        fo.write(byteBuffer);
                        byteBuffer.flip();
                    }
                    fo.close();
                }catch (Exception e) {
                    e.printStackTrace();
                }
            } else if ("mkdir".equals(s)){
                try {
                    if (args.length!=2){
                        System.out.println(usage);
                        System.exit(0);
                    }
                    String uri = getUri(args[1]);
                    simpleDistributedSystem.mkdir(uri);
                }catch (Exception e) {
                    System.out.println("Illegal uri");
                    e.printStackTrace();
                }
            }
            else if ("delete".equals(s)){
                try {
                    if (args.length!=2){
                        System.out.println(usage);
                        System.exit(0);
                    }
                    String uri = getUri(args[1]);
                    simpleDistributedSystem.delete(uri);
                }catch (Exception e) {
                    System.out.println("Illegal uri");
                    e.printStackTrace();
                }
            }
            //else if ("ls".equals(s)){
//                try {
//                    if (args.length!=2){
//                        System.out.println(usage);
//                        System.exit(0);
//                    }
//                    String uri = getUri(args[1]);
//                    List<String> list = simpleDistributedSystem.list(uri);
//                    for (String f:list
//                         ) {
//                        System.out.println(f);
//                    }
//                }catch (Exception e) {
//                    System.out.println("Illegal uri");
//                    e.printStackTrace();
//                }
//            }
            else {
                System.out.println("Illegal input");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static String getUri(String s) throws URISyntaxException {
        String reg = "^(sdfs|SDFS)://((([0-9]+.)+[0-9]+)|localhost)(:[0-9]+)*((/[\\s\\S]*)+(/)*)$";
        Pattern pattern = Pattern.compile(reg);
        Matcher matcher = pattern.matcher(s);
        if (!matcher.find())
            throw new URISyntaxException(s,"wrong fileUri");
        return matcher.group(6);
    }
}
