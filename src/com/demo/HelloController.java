package com.demo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.jfinal.core.Controller;
import com.plugin.Hbase;

public class HelloController extends Controller {
    /**
     * 进入主页面
     */
    public void index() {
        render("index.html");
    }

    /**
     * 进入添加页面
     */
    public void add() {
        render("add.html");
    }

    /**
     * 进入添加页面
     */
    public void openupdate() {
        //获取页面数据
        String rowname=getPara("rowname");

        Map<String, Object> resMap = new HashMap<String, Object>();
        //获取数据库的数据
        try {
            resMap=Hbase.get("student", rowname);
        } catch (Exception e) {
            e.printStackTrace();
        }
        setAttr("info", resMap);
        renderJsp("update.jsp");
    }

    public void indexData() {
        ArrayList<Student> studnets = new ArrayList<>();
        Student s1 = new Student();
        s1.setNo("01");
        s1.setCls("16计本1班");
        s1.setName("小米");

        Student s2 = new Student();
        s2.setNo("02");
        s2.setCls("16计本1班");
        s2.setName("小花");

        Student s3 = new Student();
        s3.setNo("01");
        s3.setCls("16计本2班");
        s3.setName("旺旺");

        studnets.add(s1);
        studnets.add(s2);
        studnets.add(s3);

        setAttr("infos", studnets);
        renderJson();
    }

    public void hbase() {

        Map<String, Object> resMap = new HashMap<String, Object>();
        try {
            resMap = Hbase.get("stu", "2");
        } catch (Exception e) {
            e.printStackTrace();
        }
        renderText(resMap.toString());
    }

    // 查询student表信息
    public void scanStudent() {
        List<Map<String, Object>> studnets = new ArrayList<Map<String, Object>>();
        try {
            studnets = Hbase.scan("student");
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        setAttr("infos", studnets);
        renderJson();
    }

    // 保存数据
    public void save() {
        // 获取页面数据
        String rowname = getPara("rowname");
        String no = getPara("no");
        String name = getPara("name");
        String cls = getPara("cls");

        List<Put> puts = new ArrayList<Put>();

        Put put1 = new Put(Bytes.toBytes(rowname));
        put1.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("no"), Bytes.toBytes(no));
        Put put2 = new Put(Bytes.toBytes(rowname));
        put2.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("name"), Bytes.toBytes(name));
        Put put3 = new Put(Bytes.toBytes(rowname));
        put3.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("cls"), Bytes.toBytes(cls));

        puts.add(put1);
        puts.add(put2);
        puts.add(put3);

        // 保存数据
        try {
            Hbase.puts(puts, "student");
        } catch (Exception e) {
            e.printStackTrace();
        }

        render("index.html");
    }

    /**
     * 删除数据
     */
    public void delete() {
        // 获取页面数据
        String rowname = getPara("rowname");
        try {
            Hbase.deleteRow("student", rowname);
        } catch (IOException e) {
            e.printStackTrace();
        }
        render("index.html");
    }
    public void update() {

    }
}