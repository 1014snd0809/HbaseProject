package com.demo;

import com.jfinal.core.JFinal;

public class Start {

    public static void main(String[] args) {
        JFinal.start("web", 8080, "/", 5);
    }

}


