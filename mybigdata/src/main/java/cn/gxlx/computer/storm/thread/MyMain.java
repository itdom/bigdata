package cn.gxlx.computer.storm.thread;

import java.util.ArrayList;
import java.util.List;

public class MyMain {

    public static void main(String[] args) {

        Hello hello = new Hello();
        hello.setId(10);

        Hello hello2 = new Hello();
        hello2.setId(20);

        List<Hello> hellos = new ArrayList<>();
        hellos.add(hello);
        hellos.add(hello2);

        MyThread myThread = new MyThread(hellos);
        myThread.start();

    }
}
