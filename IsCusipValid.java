package com.venkat.hiveGeneticUDF;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
//import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;

public class IsCusipValid extends GenericUDF {
	
	private ObjectInspector[] argumentOIs;
	private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
	private Text isValid = new Text();

	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		
		if (arguments.length > 2) {
			throw new UDFArgumentLengthException(
					"The function ValidCusip(string) needs one argument only " + "passed " + arguments.length + " argument (s) ");
			
		}

		for (int i = 0; i < arguments.length; i++) {
			if (arguments[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
				throw new UDFArgumentTypeException(i, "Only primitive type arguments are accepted but "
						+ arguments[i].getTypeName() + " is passed.");
			}
		}

		argumentOIs = arguments;
		returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
		for (int i = 0; i < arguments.length; i++) {
			if (!returnOIResolver.update(arguments[i])) {
				throw new UDFArgumentTypeException(i, "The value of return should have the same type: \""
						+ returnOIResolver.get().getTypeName() + "\" is expected but \"" 
						+ arguments[i].getTypeName()
						+ "\" is found");
			}
		}

		return returnOIResolver.get() ;				
}
	
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		
		Object CusipObj =  returnOIResolver.convertIfNecessary(arguments[0].get(), argumentOIs[0]);
		String Cusip = ""; 
		int checkDigit = 0;
		int val = 0;
		int sum = 0;
	    String validLetters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ*@#";
	    String MatchStrDecimal = "\\d" ;
		
		if (CusipObj == null )
		{
			isValid.set("");
			return isValid ;
		}
	    
		//CusipObj = returnOIResolver.convertIfNecessary(arguments[0].get(), argumentOIs[0]);
		
		Cusip = CusipObj.toString();
		
		if ( Cusip.toString() == "" ) {
			isValid.set("");
			return isValid ; 
		}
		
	    if (Cusip.toString().length() != 9 ) {
	    	isValid.set("Invalid");
	    	return isValid ;
	   	}

	    
	    if (!Cusip.substring(Cusip.length()-1).matches(MatchStrDecimal) ) {
	    	isValid.set("Invalid");
	    	return isValid ;
	   	}
		
	    for (int i=0; i<Cusip.length()-1; i++){
	    	val = validLetters.toString().indexOf(Cusip.toUpperCase().charAt(i)) ;   	
	    	if ((i+1)%2==0)
	    		val = val*2 ;
	    	 sum = sum +  (val/ 10)  + (val % 10) ;
	    }
	    checkDigit = ((10 - sum%10))%10 ;
		
	    if ( Integer.valueOf(Cusip.substring(8)) == checkDigit) {

			isValid.set("Valid");
			return isValid ;
	    }
		else { 
			isValid.set("Invalid");
			return isValid ;
	    }

	}
	
	
	
	public String getDisplayString(String[] children) {
		return "ValidCusip  (" + StringUtils.join(children, ',') + ")";
	}

}
