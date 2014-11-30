<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"
    import = "java.util.*"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Insert title here</title>
</head>
<body>
<%
HashMap hm = (HashMap) session.getAttribute("lists");
Set s = hm.entrySet();
Iterator it = s.iterator();
while(it.hasNext())
{	Map.Entry me = (Map.Entry) it.next();%>
	
	<%=me.getKey()%> &nbsp;&nbsp;
	<%=me.getValue() %> occurences <br>
	<%
}
 %>
</body>
</html>