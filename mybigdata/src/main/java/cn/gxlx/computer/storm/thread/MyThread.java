package cn.gxlx.computer.storm.thread;

import java.util.List;

public class MyThread extends Thread {

    private List<Hello> helloList;

    public MyThread(List<Hello> helloList) {
        this.helloList = helloList;
    }

    @Override
    public void run() {

        for (Hello hello : helloList) {
            System.out.println("thread:" + Thread.currentThread().getId() + hello.getId());
        }

    }
}
