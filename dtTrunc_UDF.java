package com.venkat.HiveUDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class dtTrunc extends UDF {
	private final SimpleDateFormat standardFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private final SimpleDateFormat myFormatter = new SimpleDateFormat("yyyy-MM-dd");
	
	
	public dtTrunc() {
    standardFormatter.setLenient(false);
    myFormatter.setLenient(false);
	}

	Text result = new Text();
	
	public Text evaluate(Text dateText) {
		if (dateText == null ) {
			return null;
		}
		
		Date date;
		try {
			date = standardFormatter.parse(dateText.toString());
			result.set(myFormatter.format(date));
			return result;
		} catch (ParseException e) {
			return null;
		}
	}
}