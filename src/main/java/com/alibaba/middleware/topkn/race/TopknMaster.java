package com.alibaba.middleware.topkn.race;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * TopknMaster负责接收题目给定的k,n值，并且将信息发送给TopknWorker1和TopknWorker2 Created by wanshao on 2017/6/29.
 */
public class TopknMaster implements Runnable {

    // 比赛输入
    private static long k;
    private static int n;
    private static Logger logger = LoggerFactory.getLogger(TopknMaster.class);
    // port address of com.alibaba.middleware.topkn.TopknWorker
    private int port;

    public TopknMaster(int port) {
        this.port = port;
    }

    /**
     * 初始化系统属性
     */
    private static void initProperties() {

    }

    public static void main(String[] args) {
        logger.info("init some args....");
        initProperties();
        // 获取比赛使用的k,n值
        k = Long.valueOf(args[0]);
        n = Integer.valueOf(args[1]);

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit(new TopknMaster(5527));
        executorService.submit(new TopknMaster(5528));

    }

    @Override
    public void run() {
        this.startMasterThread(port);
    }

    /**
     * @return
     * @throws IOException
     */
    private void startMasterThread(int port) {
        ServerSocketChannel serverSocket = null;
        try {

            serverSocket = ServerSocketChannel.open();
            serverSocket.bind(new InetSocketAddress(port));
            logger.info("Port 5527 and 5528 is open for connecting...");

            // 处理一次请求来回即可
            SocketChannel socketChannel = serverSocket.accept();
            socketChannel.configureBlocking(true);
            Thread writeThread = new Thread(new MasterWriteThread(socketChannel));
            Thread readThread = new Thread(new MasterReadThread(socketChannel));
            writeThread.start();
            readThread.start();
            readThread.join();

            // 处理完毕后关闭socket
            socketChannel.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    class MasterReadThread implements Runnable {

        private final Logger logger = LoggerFactory.getLogger(MasterReadThread.class);
        private final int READ_BUFFER_SIZE = 1024;
        private SocketChannel socketChannel;

        public MasterReadThread(SocketChannel socketChannel) {
            this.socketChannel = socketChannel;
        }

        @Override
        public void run() {
            getResultAndProcess();
        }

        /**
         * 获取worker的结果并且处理
         */
        private void getResultAndProcess() {

            try {
                logger.info("Begin to read result from worker " + socketChannel.getRemoteAddress());
                ByteBuffer readBuffer = ByteBuffer.allocate(READ_BUFFER_SIZE);
                while (socketChannel.read(readBuffer) != -1) {
                    readBuffer.flip();

                    // do something with the result
                    processResult(readBuffer);
                    readBuffer.clear();

                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * 对结果数据做一些处理
         */
        private void processResult(ByteBuffer readBuffer) {
            logger.info(new String(readBuffer.array()));
            System.out.println(new String(readBuffer.array()));
        }
    }

    /**
     * master可以使用该线程发送比赛数据给worker,并且读取处理结果
     */
    public class MasterWriteThread implements Runnable {

        private static final int WRITE_BUFFER_SIZE = 1024;
        private final Logger logger = LoggerFactory.getLogger(MasterWriteThread.class);
        private SocketChannel socketChannel;

        public MasterWriteThread(SocketChannel socketChannel) {
            this.socketChannel = socketChannel;
        }

        @Override
        public void run() {
            try {

                logger.info("Begin to send input to worker " + socketChannel.getRemoteAddress());
                ByteBuffer sendBuffer = ByteBuffer.allocate(WRITE_BUFFER_SIZE);
                sendBuffer.clear();
                // 发送比赛输入
                sendBuffer.putLong(k);
                sendBuffer.putInt(n);
                sendBuffer.flip();
                while (sendBuffer.hasRemaining()) {
                    socketChannel.write(sendBuffer);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
