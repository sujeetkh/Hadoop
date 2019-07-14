package in.iisc.csa.sujeet.hadoop.csv;

import org.apache.commons.lang.StringEscapeUtils;

public class TestNewLine {
public static void main(String[] args) {
String str="com.bsb.hike.modules.httpmgr.exception.HttpException: java.io.IOException\n" + 
		"	at com.bsb.hike.modules.httpmgr.d.g.a(SourceFile:388)\n" + 
		"	at com.bsb.hike.modules.httpmgr.d.g.b(SourceFile:321)\n" + 
		"	at com.bsb.hike.modules.httpmgr.d.h.b(SourceFile:463)\n" + 
		"	at com.bsb.hike.modules.httpmgr.f.a.a(SourceFile:33)\n" + 
		"	at com.bsb.hike.modules.httpmgr.d.h.b(SourceFile:457)\n" + 
		"	at com.bsb.hike.modules.b.e.a$1.a(SourceFile:141)\n" + 
		"	at com.bsb.hike.modules.httpmgr.d.h.b(SourceFile:457)\n" + 
		"	at com.bsb.hike.modules.httpmgr.d.g.d(SourceFile:101)\n" + 
		"	at com.bsb.hike.modules.httpmgr.d.g.a(SourceFile:55)\n" + 
		"	at com.bsb.hike.modules.httpmgr.d.g$1.a(SourceFile:194)\n" + 
		"	at com.bsb.hike.modules.httpmgr.j.aa.run(SourceFile:26)\n" + 
		"	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:422)\n" + 
		"	at java.util.concurrent.FutureTask.run(FutureTask.java:237)\n" + 
		"	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$201(ScheduledThreadPoolExecutor.java:152)\n" + 
		"	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:265)\n" + 
		"	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1112)\n" + 
		"	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:587)\n" + 
		"	at java.lang.Thread.run(Thread.java:818)\n" + 
		"Caused by: java.io.IOException\n" + 
		"	at com.bsb.hike.modules.httpmgr.d.g.b(SourceFile:262)\n" + 
		"	... 16 more\n" + 
		"";

	
	
	String repl = "my life";
	System.out.println(StringEscapeUtils.escapeCsv(repl));
	
	str=String.valueOf(StringEscapeUtils.escapeCsv(StringEscapeUtils.escapeJava(str)));
	System.out.println(str);
}
}
