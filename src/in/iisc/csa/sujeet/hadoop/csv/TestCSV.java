package in.iisc.csa.sujeet.hadoop.csv;

import java.io.IOException;
import java.sql.Timestamp;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;

public class TestCSV {
public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException {
	String log = "\"5ba0bd64e4b0505bddf71609\",devEvent,conn,gcm,1537260348803,,le_android,2018-09-18 08:54:59.0,\"{reconnectVal=1, extras=Bundle[{pushReconnect=1, msisdn=+919542114400, sMsgId=16233, sUid=u:WvV9-0rzkBAU-pA0, pushConnId=WvV9-0rzkBAU-pA0$Wx6MGRaUel913_qI$16233$1537260346949$134362644}], userAuthenticated=true}\",0,7.1.2,and:d941bf68a771baeadcf4f5d4110f2bdb3dd58aea,Wx6MGRaUel913_qI,+919542114500,IN,IN,idea,idea,ap,ap,-1,android-5.10.3,,1537259874640,,,,,,,,,,,,,android-5.10.3,,,7.1.2,,+919542114500,clitics,10.9.8.62,act_rel,2018-09-18,8,45";
	String strt="\"5ba0be8de4b0505bde587e5f\",asset_mngr,process,a,download,86400000,416,2018-09-18 08:59:57.0,,,AICAIAQpi7B44A,and:582fa150b83f860901908d322175f4a5b2808df6,Wg7wccvZgg-yO3ya,,,,,,,,,0,0,1537118641869,\"com.bsb.hike.modules.httpmgr.exception.HttpException: java.io.IOException\n" + 
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
			"\",3g,409c8f1d-07d7-47f9-ba64-f0b438a03b8f,failure,,,,,,,1537118727661,3,android-5.13.4,,,5.1.1,,+918880447557,clitics,10.9.8.42,act_rel,2018-09-18,8,45";
	CsvMapper mapper = new CsvMapper();
	
	String[] values=mapper.readValue(log, String[].class);
	System.out.println(values[44]);
	int i=0;
	for(String value:values)
	{
		System.out.println(i+": "+value);
		i++;
	}
	
	String str=String.format("gs://hike_staging/sujeet/dataflow_orc_data/dt=%s/hour=%s/kingdom=%s/%s_%s.orc", "2018-09-21","21","act_log","act_log","random_str");
	System.out.println(str);
	Timestamp ts= Timestamp.valueOf("2018-09-18 08:54:00.0");
	System.out.println(ts);
	
}
}
