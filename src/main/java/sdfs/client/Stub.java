package sdfs.client;/*
* Created by xk on 2017/11/17 15.
*/

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

public class Stub {
    enum Server{
        DATANODE,NAMENODE
    }
    static Object rpc(Server server, String method, Class<?>[] paraTypes, Object[] parameters, InetSocketAddress inetSocketAddress) throws IOException {
        Socket socket;
        ObjectOutputStream outputStream;
        if (server==Server.DATANODE) {
            socket = new Socket(inetSocketAddress.getHostName(), inetSocketAddress.getPort());
            outputStream = new ObjectOutputStream(socket.getOutputStream());
            outputStream.writeUTF("DataNodeServer");
        }else// if (server==Server.NAMENODE)
        {
            socket = new Socket(inetSocketAddress.getHostName(),inetSocketAddress.getPort());
            outputStream = new ObjectOutputStream(socket.getOutputStream());
            outputStream.writeUTF("NameNodeServer");
        }
        outputStream.writeUTF(method);
        outputStream.writeObject(paraTypes);

        outputStream.writeObject(parameters);
        outputStream.flush();
        ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
        Object b = null;
        try {
            b = inputStream.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        outputStream.close();
        inputStream.close();
        socket.close();
        return b;
    }

}