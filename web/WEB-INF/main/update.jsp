<%@ page language="java" contentType="text/html; charset=utf-8"
         pageEncoding="utf-8"%>
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>修改信息页面</title>
</head>
<body>

请修改学生信息<br>
<form action="/hello/save" method="post">
    行号：<input type="text" name="rowname" value="${info.rowname}"><br>
    学号：<input type="text" name="no"  value="${info.base_info.no}"><br>
    姓名：<input type="text" name="name"  value="${info.base_info.name}"><br>
    班级：<input type="text" name="cls"  value="${info.base_info.cls}"><br>
    <input type="submit" value="保存">
</form>
</body>
</html>